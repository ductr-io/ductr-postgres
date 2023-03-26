# frozen_string_literal: true

module Ductr
  module Postgres
    #
    # A source control that yields rows usnig the PostgreSQL streaming feature, registered as `:streamed`:
    #
    #   source :some_postgres_database, :streamed
    #   def select_some_stuff(db)
    #     db[:items].limit(42)
    #   end
    #
    # You can select a large number of rows, without worrying about pagination handling or memory usage.
    #
    class StreamedSource < Ductr::ETL::Source
      Adapter.source_registry.add(self, as: :streamed)

      #
      # Opens the database, calls the job's method and iterate over the query results.
      #
      # @yield The each block
      #
      # @return [void]
      #
      def each(&)
        call_method(adapter.db).each(&)
      end
    end
  end
end
