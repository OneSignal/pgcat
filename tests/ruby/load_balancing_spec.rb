# frozen_string_literal: true
require_relative "spec_helper"

describe "Random Load Balancing" do
  let(:processes) { Helpers::Pgcat.single_shard_setup("sharded_db", 5) }
  after do
    processes.all_databases.map(&:reset)
    processes.pgcat.shutdown
  end

  context("under regular circumstances") do
    it "balances query volume between all instances" do
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))

      query_count = QUERY_COUNT
      expected_share = query_count / processes.all_databases.count
      failed_count = 0

      query_count.times do
        conn.async_exec("SELECT 1 + 2")
      rescue
        failed_count += 1
      end

      expect(failed_count).to(eq(0))
      processes.all_databases.map(&:count_select_1_plus_2).each do |instance_share|
        expect(instance_share).to(be_within(expected_share * MARGIN_OF_ERROR).of(expected_share))
      end
    end
  end

  context("when some replicas are down") do
    it "balances query volume between working instances" do
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
      expected_share = QUERY_COUNT / (processes.all_databases.count - 2)
      failed_count = 0

      processes[:replicas][0].take_down do
        processes[:replicas][1].take_down do
          QUERY_COUNT.times do
            conn.async_exec("SELECT 1 + 2")
          rescue
            conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
            failed_count += 1
          end
        end
      end

      processes.all_databases.each do |instance|
        queries_routed = instance.count_select_1_plus_2
        if processes.replicas[0..1].include?(instance)
          expect(queries_routed).to(eq(0))
        else
          expect(queries_routed).to(be_within(expected_share * MARGIN_OF_ERROR).of(expected_share))
        end
      end
    end
  end
end

describe "Least Outstanding Queries Load Balancing" do
  let(:processes) { Helpers::Pgcat.single_shard_setup("sharded_db", 1, "transaction", "loc") }
  after do
    processes.all_databases.map(&:reset)
    processes.pgcat.shutdown
  end

  context("under homogeneous load") do
    it "balances query volume between all instances" do
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))

      query_count = QUERY_COUNT
      expected_share = query_count / processes.all_databases.count
      failed_count = 0

      query_count.times do
        conn.async_exec("SELECT 1 + 2")
      rescue
        failed_count += 1
      end

      expect(failed_count).to(eq(0))
      processes.all_databases.map(&:count_select_1_plus_2).each do |instance_share|
        expect(instance_share).to(be_within(expected_share * MARGIN_OF_ERROR).of(expected_share))
      end
    end
  end

  context("under heterogeneous load") do
    xit("balances query volume between all instances based on how busy they are") do
      slow_query_count = 2
      threads = Array.new(slow_query_count) do
        Thread.new do
          conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
          conn.async_exec("BEGIN")
        end
      end

      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))

      query_count = QUERY_COUNT
      expected_share = query_count / (processes.all_databases.count - slow_query_count)
      failed_count = 0

      query_count.times do
        conn.async_exec("SELECT 1 + 2")
      rescue
        failed_count += 1
      end

      expect(failed_count).to(eq(0))
      # Under LOQ, we expect replicas running the slow pg_sleep
      # to get no selects
      expect(
        processes
          .all_databases
          .map(&:count_select_1_plus_2)
          .count { |instance_share| instance_share == 0 }
      )
        .to(eq(slow_query_count))

      # We also expect the quick queries to be spread across
      # the idle servers only
      processes
        .all_databases
        .map(&:count_select_1_plus_2)
        .reject { |instance_share| instance_share == 0 }
        .each do |instance_share|
          expect(instance_share).to(be_within(expected_share * MARGIN_OF_ERROR).of(expected_share))
        end

      threads.map(&:join)
    end
  end

  context("when some replicas are down") do
    it "balances query volume between working instances" do
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
      expected_share = QUERY_COUNT / (processes.all_databases.count - 2)
      failed_count = 0

      processes[:replicas][0].take_down do
        processes[:replicas][1].take_down do
          QUERY_COUNT.times do
            conn.async_exec("SELECT 1 + 2")
          rescue
            conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
            failed_count += 1
          end
        end
      end

      expect(failed_count).to(be <= 2)
      processes.all_databases.each do |instance|
        queries_routed = instance.count_select_1_plus_2
        if processes.replicas[0..1].include?(instance)
          expect(queries_routed).to(eq(0))
        else
          expect(queries_routed).to(be_within(expected_share * MARGIN_OF_ERROR).of(expected_share))
        end
      end
    end
  end
end

describe "Candidate filtering based on `default_pool`" do
  let(:processes) {
    Helpers::Pgcat.single_shard_setup("sharded_db", 5, "transaction", "random", "debug", pool_settings, general_settings)
  }

  after do
    processes.all_databases.map(&:reset)
    processes.pgcat.shutdown
  end

  context("with default_pool set to replicas") do
    context("when all replicas are down ") do
      let(:ban_time) { 60 }
      let(:connect_timeout) { 1000 }
      let(:pool_settings) do
        {
          "default_role" => "replica",
          "replica_to_primary_failover_enabled" => replica_to_primary_failover_enabled,
          "connect_timeout" => connect_timeout,
        }
      end
      let(:general_settings) { { "ban_time" => ban_time } }

      context("with `replica_to_primary_failover_enabled` set to false`") do
        let(:replica_to_primary_failover_enabled) { false }

        it(
          "unbans them automatically to prevent false positives in health checks that could make all replicas unavailable"
        ) do
          conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
          failed_count = 0
          number_of_replicas = processes[:replicas].length

          # Take down all replicas
          processes[:replicas].each(&:take_down)

          (number_of_replicas + 1).times do |n|
            conn.async_exec("SELECT 1 + 2")
          rescue
            conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
            failed_count += 1
          end

          expect(failed_count).to(eq(number_of_replicas + 1))
          failed_count = 0

          # Ban_time is configured to 60 so this reset will only work
          # if the replicas are unbanned automatically
          processes[:replicas].each(&:reset)

          number_of_replicas.times do
            conn.async_exec("SELECT 1 + 2")
          rescue
            conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
            failed_count += 1
          end

          expect(failed_count).to(eq(0))
        end
      end

      context("with `replica_to_primary_failover_enabled` set to true`") do
        let(:replica_to_primary_failover_enabled) { true }

        it "does not unbans them automatically" do
          conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
          failed_count = 0
          number_of_replicas = processes[:replicas].length

          # We need to allow pgcat to open connections to replicas
          (number_of_replicas + 10).times do |n|
            conn.async_exec("SELECT 1 + 2")
          rescue
            conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
            failed_count += 1
          end
          expect(failed_count).to(eq(0))

          # Take down all replicas
          processes[:replicas].each(&:take_down)

          (number_of_replicas + 10).times do |n|
            conn.async_exec("SELECT 1 + 2")
          rescue
            conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
            failed_count += 1
          end

          expect(failed_count).to(eq(number_of_replicas))
        end

        context("when banned replicas are tested for availability because they expired the ban time") do
          let(:ban_time) { 2 }
          it "should be done in the background without interfering with traffic" do
            select_server_port = "SELECT setting AS port FROM pg_settings WHERE name = 'port';"
            conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
            primary_port = processes.primary.original_port;

            response = conn.async_exec(select_server_port)
            expect(response[0]["port"].to_i).not_to(eq(primary_port))

            failed_count = 0
            number_of_replicas = processes[:replicas].length

            # We need to allow pgcat to open connections to replicas
            (number_of_replicas + 10).times do |n|
              response = conn.async_exec(select_server_port)
              expect(response[0]["port"].to_i).not_to(eq(primary_port))
            rescue
              conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
              failed_count += 1
            end
            expect(failed_count).to(eq(0))

            # Take down all replicas
            processes[:replicas].each(&:take_down)

            (number_of_replicas).times do |n|
              response = conn.async_exec(select_server_port)
              expect(response[0]["port"].to_i).to(eq(primary_port))
            rescue
              conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
              failed_count += 1
            end
            expect(failed_count).to(eq(number_of_replicas))
            failed_count = 0

            response = conn.async_exec(select_server_port)
            expect(response[0]["port"].to_i).to(eq(primary_port))
            sleep(ban_time + 1)

            begin
              time_before_query = Time.now
              response = conn.async_exec(select_server_port)
              expect(response[0]["port"].to_i).to(eq(primary_port))
              expect(Time.now - time_before_query).to(be < connect_timeout / 1000.0)
            rescue
              conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
              failed_count += 1
            end
            expect(response[0]["port"].to_i).to(eq(primary_port))
            expect(failed_count).to(eq(0))
          end

          it "should unban replicas if they become available after the ban time" do
            select_server_port = "SELECT setting AS port FROM pg_settings WHERE name = 'port';"
            conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
            failed_count = 0
            primary_port = processes.primary.original_port;

            number_of_replicas = processes[:replicas].length

            # We need to allow pgcat to open connections to replicas
            (number_of_replicas + 10).times do |n|
              response = conn.async_exec(select_server_port)
              expect(response[0]["port"].to_i).not_to(eq(primary_port))
            rescue
              conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
              failed_count += 1
            end
            expect(failed_count).to(eq(0))

            # Take down all replicas
            processes[:replicas].each(&:take_down)

            (number_of_replicas).times do |n|
              conn.async_exec("SELECT 1 + 2")
            rescue
              conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
              failed_count += 1
            end
            expect(failed_count).to(eq(number_of_replicas))
            failed_count = 0

            processes[:replicas].each(&:reset)
            sleep(ban_time + 1)
            response = nil
            number_of_replicas.times do
              response = conn.async_exec("SELECT 1 + 2")
            rescue
              conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
              failed_count += 1
            end
            expect(response[0]["port"].to_i).not_to(eq(primary_port))
            expect(failed_count).to(eq(0))
          end
        end
      end
    end
  end
end
