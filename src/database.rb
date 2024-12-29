require '/bundle/setup'
require 'simple_ruby_db'


class Database
  private attr_reader :db, :planner

  def initialize
    data_file_path = "#{Dir.pwd}/datafile"
    meta_file_path = "#{Dir.pwd}/metafile"

    File.open(data_file_path, 'wb') { |f| } unless File.exist?(data_file_path)
    File.open(meta_file_path, 'wb') { |f| } unless File.exist?(meta_file_path)

    @db = SimpleRubyDb::SimpleDb.new(data_file_path, meta_file_path)
    @planner = @db.planner
  end

  def run(sql)
    puts "SQL: #{sql}"
    case sql
    in String if sql.start_with?('create table')
      create_table(sql)
    in String if sql.start_with?('insert')
      insert(sql)
    in String if sql.start_with?('update')
      update(sql)
    in String if sql.start_with?('select')
      result = select(sql)
      puts "Result: #{result}"
    else
      return
    end
  end

  private

  def create_table(sql)
    planner.execute_update(sql, db.metadata_buffer_pool_manager)
  end

  def insert(sql)
    planner.execute_update(sql, db.buffer_pool_manager)
  end

  def update(sql)
    planner.execute_update(sql, db.buffer_pool_manager)
  end

  def select(sql)
    scan = db.planner.create_query_plan(sql, db.buffer_pool_manager).open

    schema = db.metadata_manager.layout(table_name(sql)).schema
    request_field_list = field_list(sql)
    col_defs = request_field_list.map { |field_name| {name: field_name, type: schema.field_type(field_name)} }

    [].tap do |result|
      while scan.next
        result.push(col_values(col_defs, scan))
      end
    end
  end

  def table_name(sql)
    # 今回、テーブル名は1つしか指定できない仕様なので、 first で取得して問題ない
    SimpleRubyDb::Parse::Parser.new(sql).query.table_list.first
  end

  def field_list(sql)
    SimpleRubyDb::Parse::Parser.new(sql).query.field_list
  end

  def col_values(col_defs, scan)
    col_defs.map do |col_def|
      col_def[:type] == 'integer' ? scan.get_int(col_def[:name]) : scan.get_string(col_def[:name]).delete_prefix("'").delete_suffix("'")
    end
  end
end

database = Database.new
ARGV.each do |sql|
  database.run(sql)
  puts '=' * 20
end


