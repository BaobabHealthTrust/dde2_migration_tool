#!/usr/bin/env ruby
require "rubygems"
require "json"
require "yaml"
require "fileutils"
require "rest-client"
require "./national_patient_id"

module Seeding

  class Split

    def load_seed_data

      puts "ERROR: Configuration file not found. Aborting!" and return if !File.exists?("./databases.yml")

      settings = YAML.load_file("./databases.yml")["npids_mysql_source"] rescue {}

      host = settings["host"]
      port = settings["port"]
      db = settings["database"]
      username = settings["username"]
      password = settings["password"]
      table = settings["table"]
      field = settings["field"]

      puts "ERROR: Configuration file not complete. Aborting!" and return if host.nil? or port.nil? or db.nil? or username.nil? or password.nil? or table.nil? or field.nil?

      system("clear")

      puts "Checking if required files available..."

      Dir.mkdir("./tmp") if !File.exists?("./tmp")

      system("clear")

      puts "Extracting data from MySQL database..."

      `mysql -h #{host} -P #{port} -u #{username} -p#{password} #{db} -e "SELECT DISTINCT(#{field}) FROM #{table}" > ./tmp/integrated_npids.txt`

      system("clear")

      puts "Removing unwanted data in extract..."

      `sed -i '/npid/d' ./tmp/integrated_npids.txt`

      system("clear")

      puts "Done!"

    end

    def split_data

      system("clear")

      puts "Checking if required files available..."

      if !File.exists?("./tmp/integrated_npids.txt")

        system("clear")

        puts "Necessary input files not found. Generating..."

        self.load_seed_data

      end

      lines = File.open("./tmp/integrated_npids.txt").read.split("\n")

      i = 0
      j = 0
      k = 0
      size = 500000

      group = []

      FileUtils.rm_rf("./seed") if File.exists?("./seed")

      Dir.rmdir("./seed") if File.exists?("./seed")

      Dir.mkdir("./seed") if !File.exists?("./seed")

      system("clear")

      puts "Generating data files..."

      puts

      lines.shuffle.each do |line|

        $stdout.write "\rProgress: #{"% 3d" % (k * 100 / lines.length)}%"

        $stdout.flush

        if j == 0

          group = []

          file = File.open("./seed/file#{i}.json", "w")

          file.write('{"docs":[')

          file.close

        end

        valid = NationalPatientId.valid?(NationalPatientId.to_decimal(line.strip)) rescue false

        next if !valid

        k += 1

        str = "{\"national_id\":\"#{line}\",\"type\":\"Npid\",\"_id\":\"#{k}\"}"

        group << str

        j += 1

        if j == size

          file = File.open("./seed/file#{i}.json", "a")

          file.write(group.join(",\n"))

          file.write("]}")

          file.close

          j = 0

          i += 1

        end

      end

      file = File.open("./seed/file#{i}.json", "a")

      file.write(group.join(",\n"))

      file.write("]}")

      file.close

      system("clear")

      puts "Done!"

      puts

    end


    def initialize_npid_data

      system("clear")

      puts "ERROR: Configuration file not found. Aborting!" and return if !File.exists?("./databases.yml")

      settings = YAML.load_file("./databases.yml")["couchdb"] rescue {}

      host = settings["host"]
      port = settings["port"]
      npids_db = settings["npids_database"]
      person_db = settings["person_database"]
      username = settings["username"]
      password = settings["password"]

      puts "ERROR: Configuration file not complete. Aborting!" and return if host.nil? or port.nil? or npids_db.nil? or username.nil? or password.nil? or person_db.nil?

      system("clear")

      puts "Checking if NPIDs database exists..."

      Dir.mkdir("./log") if !File.exists?("./log")

      npid_db_check = JSON.parse(`curl "http://#{host}:#{port}/#{npids_db}" -s`)

      if !npid_db_check["error"].nil?

        system("clear")

        puts "NPIDs database not found. Creating database..."

        result = `curl -X PUT http://#{username}:#{password}@#{host}:#{port}/#{npids_db}`

        system("clear")

        puts "NPID database creation result..."

        puts

        puts result

        sleep 1

      end

      system("clear")

      puts "Creating NPID Database views in CouchDB..."

      puts

      `curl -H "Content-Type:application/json" -X POST -d @./views/npid_views.json http://#{username}:#{password}@#{host}:#{port}/#{npids_db}/_bulk_docs -s > ./log/npid_views.log`

      system("clear")

      puts "Checking if Person database exists..."

      person_db_check = JSON.parse(`curl "http://#{host}:#{port}/#{person_db}" -s`)

      if !person_db_check["error"].nil?

        system("clear")

        puts "Person database not found. Creating database..."

        result = `curl -X PUT http://#{username}:#{password}@#{host}:#{port}/#{person_db}`

        system("clear")

        puts "Person database creation result..."

        puts

        puts result

        sleep 1

      end

      system("clear")

      puts "Creating Person Database views in CouchDB..."

      puts

      `curl -H "Content-Type:application/json" -X POST -d @./views/person_views.json http://#{username}:#{password}@#{host}:#{port}/#{person_db}/_bulk_docs -s > ./log/person_views.log`

      system("clear")

      puts "Checking if required files available..."

      if !File.exists?("./seed") or !File.exists?("./seed/file0.json")

        system("clear")

        puts "Necessary input files not found. Generating..."

        self.split_data

      end

      system("clear")

      puts "Uploading files into CouchDB..."

      puts

      Dir.foreach("./seed") do |file|
        next if file == '.' or file == '..'

        $stdout.write "\rUploading #{file}..."

        $stdout.flush

        `curl -H "Content-Type:application/json" -X POST -d @./seed/#{file} http://#{host}:#{port}/#{npids_db}/_bulk_docs -s > ./log/seed_#{file}.log`

      end

      system("clear")

      puts "Done uploading files into CouchDB..."

      puts

      sleep 1

      system("clear")

      puts "Initializing NPID views in CouchDB..."

      puts

      `curl "http://#{host}:#{port}/#{npids_db}/_design/Npid/_view/all -s"`

      system("clear")

      puts "Initializing Person views in CouchDB..."

      puts

      `curl "http://#{host}:#{port}/#{person_db}/_design/Person/_view/all -s"`

      system("clear")

      puts "Initializing Connection views in CouchDB..."

      puts

      `curl "http://#{host}:#{port}/#{person_db}/_design/Connection/_view/all -s"`

      system("clear")

      puts "Initializing Footprint views in CouchDB..."

      puts

      `curl "http://#{host}:#{port}/#{person_db}/_design/Footprint/_view/all -s"`

      system("clear")

      puts "Initializing Site views in CouchDB..."

      puts

      `curl "http://#{host}:#{port}/#{person_db}/_design/Site/_view/all -s"`

      system("clear")

      puts "Done!"

      puts


    end

  end

end

