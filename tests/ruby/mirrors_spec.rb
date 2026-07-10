# frozen_string_literal: true
require 'uri'
require_relative 'spec_helper'
require_relative 'helpers/auth_query_helper'

describe "Query Mirroing" do
  let(:processes) { Helpers::Pgcat.single_instance_setup("sharded_db", 10) }
  let(:mirror_pg) { PgInstance.new(8432, "sharding_user", "sharding_user", "shard2")}
  let(:pgcat_conn_str) { processes.pgcat.connection_string("sharded_db", "sharding_user") }
  let(:mirror_host) { "localhost" }

  before do
    new_configs = processes.pgcat.current_config
    new_configs["pools"]["sharded_db"]["shards"]["0"]["mirrors"] = [
      [mirror_host, mirror_pg.port.to_i, 0],
      [mirror_host, mirror_pg.port.to_i, 0],
      [mirror_host, mirror_pg.port.to_i, 0],
    ]
    processes.pgcat.update_config(new_configs)
    processes.pgcat.reload_config
  end

  after do
    processes.all_databases.map(&:reset)
    mirror_pg.reset
    processes.pgcat.shutdown
  end

  xit "can mirror a query" do
    conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
    runs = 15
    runs.times { conn.async_exec("SELECT 1 + 2") }
    sleep 0.5
    expect(processes.all_databases.first.count_select_1_plus_2).to eq(runs)
    # Allow some slack in mirroring successes
    expect(mirror_pg.count_select_1_plus_2).to be > ((runs - 5) * 3)
  end

  context "when main server connection is closed" do
    it "closes the mirror connection" do
      baseline_count = processes.all_databases.first.count_connections
      5.times do |i|
        # Force pool cycling to detect zombie mirror connections
        new_configs = processes.pgcat.current_config
        new_configs["pools"]["sharded_db"]["idle_timeout"] = 5000 + i
        new_configs["pools"]["sharded_db"]["shards"]["0"]["mirrors"] = [
          [mirror_host, mirror_pg.port.to_i, 0],
          [mirror_host, mirror_pg.port.to_i, 0],
          [mirror_host, mirror_pg.port.to_i, 0],
        ]
        processes.pgcat.update_config(new_configs)
        processes.pgcat.reload_config
      end
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
      conn.async_exec("SELECT 1 + 2")
      sleep 0.5
      # Expect same number of connection even after pool cycling
      expect(processes.all_databases.first.count_connections).to be < baseline_count + 2
    end
  end

  xcontext "when mirror server goes down temporarily" do
    it "continues to transmit queries after recovery" do
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
      mirror_pg.take_down do
        conn.async_exec("SELECT 1 + 2")
        sleep 0.1
      end
      10.times { conn.async_exec("SELECT 1 + 2") }
      sleep 1
      expect(mirror_pg.count_select_1_plus_2).to be >= 2
    end
  end

  context "when a mirror is down" do
    let(:mirror_host) { "badhost" }

    it "does not fail to send the main query" do
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
      # No Errors here
      conn.async_exec("SELECT 1 + 2")
      expect(processes.all_databases.first.count_select_1_plus_2).to eq(1)
    end

    it "does not fail to send the main query (even after thousands of mirror attempts)" do
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
      # No Errors here
      1000.times { conn.async_exec("SELECT 1 + 2") }
      expect(processes.all_databases.first.count_select_1_plus_2).to eq(1000)
    end
  end
end

describe "Query Mirroring with Auth Query" do
  let(:pg_user) { { 'username' => 'sharding_user', 'password' => 'sharding_user' } }
  let(:config_user) { {'username' => 'md5_auth_user'} }
  let(:auth_query_user) { { 'username' => 'md5_auth_user', 'password' => 'hash' } }
  let(:mirror_pg) { PgInstance.new(8432, "md5_auth_user", "hash", "shard0") }
  let(:mirror_host) { "localhost" }
  let(:config) {
    {
      'general' => {
        'auth_query' => "SELECT * FROM public.user_lookup('$1');",
        'auth_query_user' => auth_query_user['username'],
        'auth_query_password' => auth_query_user['password']
      },
    }
  }
  let(:processes) { Helpers::AuthQuery.single_shard_auth_query(
    pool_name: "sharded_db",
    pg_user: pg_user,
    config_user: config_user,
    extra_conf: config,
    wait_until_ready: false,
  )}

  before do
    pgcat = processes.pgcat

    # pgcat is started without waiting for readiness (auth_query isn't set up yet, so it
    # can't become ready). Wait for it to finish its initial boot before rewriting its
    # config file below, otherwise we can race pgcat's own startup read of that file.
    while !(pgcat.logs =~ /Waiting for clients/) do
      sleep 0.5
    end

    # Reloading the config to add mirrors can trigger pgcat to eagerly warm up
    # backend connections (including the mirror's), which needs auth_query to
    # already be able to authenticate against every target. Set that up before
    # the reload, not after, so we don't race pgcat's own warm-up connections:
    # a mirror connection that fails on its first attempt is never retried for
    # the lifetime of the backend connection it's attached to.
    Helpers::AuthQuery.set_up_auth_query_for_user(
      user: auth_query_user['username'],
      password: auth_query_user['password'],
      instance_ports: [processes.primary.port, processes.replicas[0].port, mirror_pg.port],
    )

    new_configs = processes.pgcat.current_config
    new_configs["pools"]["sharded_db"]["shards"]["0"]["mirrors"] = [
      [mirror_host, mirror_pg.port.to_i, 0],
      [mirror_host, mirror_pg.port.to_i, 1],
    ]
    pgcat.update_config(new_configs)
    pgcat.reload_config

    pgcat.wait_until_ready(
      pgcat.connection_string("sharded_db", auth_query_user['username'], auth_query_user['password'])
    )

    mirror_pg.reset
  end

  after do
    Helpers::AuthQuery.tear_down_auth_query_for_user(
      user: auth_query_user['username'],
      password: auth_query_user['password'],
      instance_ports: [processes.primary.port, processes.replicas[0].port, mirror_pg.port],
    )
  end

  context "when auth_query is configured" do
    it "can mirror a query" do
      runs = 5
      mirrored = 0

      # A backend connection's mirror thread only gets one attempt to connect
      # to the mirror; if that attempt fails for any transient reason, it
      # gives up for that connection's whole lifetime instead of retrying
      # (mirroring is fire-and-forget by design). If we land on a poisoned
      # backend connection, force a pool cycle -- the same trick the "closes
      # the mirror connection" spec above uses -- to get a fresh one, and
      # try again. How long any of this takes depends entirely on how fast
      # this environment can do real connect+auth round trips, which can
      # vary enormously between an idle dev box and a CPU-constrained CI
      # runner, so give every step a generous ceiling rather than a fixed
      # sleep tuned to whatever machine happened to run this last.
      5.times do |attempt|
        if attempt > 0
          new_configs = processes.pgcat.current_config
          new_configs["pools"]["sharded_db"]["idle_timeout"] = 5000 + attempt
          processes.pgcat.update_config(new_configs)
          processes.pgcat.reload_config
          sleep 1
        end

        mirror_pg.reset

        conn = PG.connect(processes.pgcat.connection_string("sharded_db", auth_query_user['username'], auth_query_user['password']))
        runs.times { conn.sync_exec("SELECT 1 + 2") }
        conn.close

        # Mirroring is fire-and-forget, so the mirror connection can still be
        # catching up right after the main queries complete. Poll instead of
        # asserting on a single snapshot.
        20.times do
          mirrored = mirror_pg.count_select_1_plus_2
          break if mirrored >= runs
          sleep 1
        end

        break if mirrored >= runs
      end

      # I'd like to check verify the primary and replica received the queries too, but getting the permissions correct is annoying
      # expect((processes.all_databases + processes.replicas).map(&:count_select_1_plus_2).sum).to eq(0)
      expect(mirrored).to eq(runs)
    end
  end
end

