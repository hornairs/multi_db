module MultiDb
  module LagMonitor

    # How long, after doing a write, should all reads be sent to the master?
    def self.sticky_master_duration # in seconds
      # these factors are largely arbitrary. the 1 exists so that even
      # if the slave is reporting no lag, we still do the next reads from master.
      (slave_lag * 1.4) + 1
    end

    # In exceptionally slow replication scenarios, we'd rather just redirect
    # everything to master and fail hard than show especially inconsistent
    # application state.
    def self.all_reads_from_master?
      slave_lag > 20
    end

    private

    def self.slave_lag(connection_proxy = ActiveRecord::Base.connection)
      current = connection_proxy.current
      return 0 if current == ActiveRecord::Base # master behind master? unlikely.

      Rails.cache.fetch("slave_lag:#{current.name}", :expires_in => 10.seconds) {
        actual_slave_lag(current)
      }
    end

    def self.actual_slave_lag(connection)
      result = connection.execute("SHOW SLAVE STATUS")
      index = result.fields.index("Seconds_Behind_Master")
      result[0].try(:[], index).to_i
    end

  end
end
