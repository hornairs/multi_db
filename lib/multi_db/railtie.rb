require 'rails/railtie'

module MultiDb
  class Railtie < ::Rails::Railtie

    initializer 'multi_db.insert' do
      slaves = MultiDb.init_slaves
      raise "No slaves databases defined for environment: #{Rails.env}" if slaves.empty?

      ActiveRecord::Base.send     :include, MultiDb::ActiveRecordExtensions
      ActionController::Base.send :include, MultiDb::Session

      Rails.application.config.after_initialize do
        ActiveRecord::Observer.send :include, MultiDb::ObserverExtensions
      end

      proxy = MultiDb::ConnectionProxy.new(ActiveRecord::Base, slaves)
      ActiveRecord::Base.connection_proxy = proxy

      ActiveRecord::Base.logger.info("** multi_db with master and #{slaves.length} slave#{"s" if slaves.length > 1} loaded.")
    end

  end
end
