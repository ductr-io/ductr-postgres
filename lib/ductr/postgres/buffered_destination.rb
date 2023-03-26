# frozen_string_literal: true

module Ductr
  module Postgres
    #
    # A destination control that accumulates rows in a buffer to write them by batch, registered as `:buffered`.
    # Accept the `:buffer_size` option, default value is 10 000:
    #
    #   destination :some_postgres_database, :buffered, buffer_size: 42
    #   def my_destination(db, buffer)
    #     db[:items].multi_insert(buffer)
    #   end
    #
    # @see more Ductr::ETL::BufferedDestination
    #
    class BufferedDestination < Ductr::ETL::BufferedDestination
      Adapter.destination_registry.add(self, as: :buffered)

      #
      # Open the database if needed and call the job's method to run the query.
      #
      # @return [void]
      #
      def on_flush
        call_method(adapter.db, buffer)
      end
    end
  end
end