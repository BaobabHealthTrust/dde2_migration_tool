#!/usr/bin/env ruby
require "rubygems"
require "json"
require "rest-client"
require "yaml"
require "./national_patient_id"
require "./utils"

module Merges

  class Merge

    attr :host
    attr :db
    attr :username
    attr :password

    def initialize(host, username, password, db)

      @host = host
      @username = username
      @password = password
      @patients = {}

      @db = db

    end

    def dump_data(table, folder, filtered=false)

      system("clear")

      puts "Dumping table '#{table}' for database '#{folder}'..."

      Utils::Files.add_directory("./data", folder)

      if table == "person_name"

        `mysql -h #{@host} -u #{@username} -p#{@password} #{@db} -e "SELECT CONCAT(person_id, '|', void_reason) FROM person_name where voided = 1 and void_reason like 'merged%'" > ./data/#{folder}/person_name.sql`

      elsif table == "patient_identifier_type"

        `mysql -h #{@host} -u #{@username} -p#{@password} #{@db} -e "SELECT CONCAT(patient_identifier_type_id, '|', name) FROM openmrs_kawale.patient_identifier_type" > ./data/#{folder}/patient_identifier_type.sql`

      end

    end

    def load_site_data(folder, filtered=false)

      tables = ["person_name", "patient_identifier_type"]

      tables.each do |table|

        dump_data(table, folder, filtered)

      end

      system("clear")

      puts "Done dumping data..."

    end

  end

  class ProcessData

    attr :patients
    attr :path

    attr :host
    attr :db
    attr :username
    attr :password

    attr :current_folder
    attr :file_limit

    attr :id_types

    def initialize(folder, host, username, password, db, file_limit=50000)

      if !File.exists?("./data/#{folder}")

        @valid = false

        return {"error" => "Target source does not exist!"}

      end

      @path = "./data/#{folder}"

      @patients = {}

      @host = host
      @username = username
      @password = password
      @patients = {}

      @db = db

      @current_folder = folder

      @id_types = {}

      @file_limit = file_limit

    end

    def load_id_types

      if File.exists?("#{@path.gsub(/\/$/, "")}/patient_identifier_type.sql")

        rows = File.open("#{@path.gsub(/\/$/, "")}/patient_identifier_type.sql").readlines

        rows.each do |row|

          values = row.split("|")

          if values.length == 2

            @id_types[values[0].strip] = values[1].strip

          end

        end

      end

    end

    def load_patients(dde_host, dde_port, dde_npid_db, dde_person_db, dde_user, dde_pass)

      if File.exists?("#{@path.gsub(/\/$/, "")}/person_name.sql")

        system("clear")

        puts "Loading identifier types..."

        load_id_types

        system("clear")

        puts "Extracting merged records..."

        puts

        i = 0
        j = 0
        k = 0

        lines = File.open("#{@path.gsub(/\/$/, "")}/person_name.sql").readlines

        hash = {}

        npid_type_id = `mysql -h #{@host} -u #{@username} -p#{@password} #{@db} -e "SELECT patient_identifier_type_id FROM patient_identifier_type WHERE name = #{pid} 'National id'"` rescue nil

        # Attempt to get the actual identifier id or default to 3 which is common
        npid_type_id = /(\d+)/.match(npid_type_id).captures.first rescue 3

        if !File.exists?("./merges/#{@current_folder}")

          Utils::Files.add_directory("./merges", "#{@current_folder}")

        end

        file = File.open("./merges/#{@current_folder}/#{@current_folder}_#{i}.json", "w")

        file.write('{"docs":[')

        file.close

        file = File.open("./merges/#{@current_folder}/#{@current_folder}_npids_#{i}.json", "w")

        file.write('{"docs":[')

        file.close

        written_npids = {}
        written_voids = {}
        written_skips = {}
        written_merged = {}

        lines.each do |line|

          k += 1

          $stdout.write "\rProgress: #{"% 3d" % (k * 100 / lines.length)}%"

          $stdout.flush

          sid, pid = /^(\d+)[^\d]+(\d+)/.match(line).captures rescue [nil, nil]

          next if sid.nil? or pid.nil?

          hash[pid] = {"national_id" => nil, "pids" => {}, "sids" => {}, "voided" => {}, "other_ids" => {}} if hash[pid].nil?

          # All identifiers
          pids = `mysql -h #{@host} -u #{@username} -p#{@password} #{@db} -e "SELECT DISTINCT(CONCAT(identifier, '|', identifier_type)) AS identifiers FROM patient_identifier WHERE patient_id = #{pid}"` rescue nil

          if pids.nil?

            Utils::Files.log("#{@current_folder}_skips", "SKIPPED", "Skipped primary patient #{pid} due to MySQL query failure")

            hash.delete(pid)

            next

          end

          npid_set = false

          if pids.strip.length > 0 and pids.match(/identifier/)

            pids = pids.split("\n")

            (1..(pids.length - 1)).each do |id|

              parts = pids[id].split("|")

              if parts.length < 2

                Utils::Files.log("#{@current_folder}_minor_skips", "SKIPPED", "Skipped #{pids[id]} due to problem with IDs mapping")

                next

              end

              # Initially removed '-'s but these are usually available in ARV numbers only which are sometimes wrongly placed as National IDs
              npid = parts[0]

              valid = NationalPatientId.valid?(NationalPatientId.to_decimal(npid)) rescue false

              if valid and npid.strip.length == 6 and parts[1].strip == npid_type_id.to_s.strip

                result = JSON.parse(RestClient.get("http://#{dde_host}:#{dde_port}/#{dde_person_db}/_design/Person/_view/by__id?key=%22#{npid.strip}%22&reduce=false&include_docs=true"))["rows"] rescue []

                if npid_set == false and result.length > 0

                  hash[pid]["national_id"] = parts[0]

                else

                  hash[pid]["other_ids"][parts[0]] = (@id_types[parts[1].strip] == "National id" ? "Old Identification Number" : @id_types[parts[1].strip])

                  hash[pid]["voided"][parts[0]] = true

                end

              else

                hash[pid]["other_ids"][parts[0]] = (@id_types[parts[1].strip] == "National id" ? "Old Identification Number" : @id_types[parts[1].strip])

              end

              # We also need to check the length of the npid as some V3 npids are also passing the basic test e.g. 'P126500005637' 
              hash[pid]["pids"][[@id_types[parts[1].strip], parts[0]]] = (valid and npid_set == false and npid.strip.length == 6 and parts[1].strip == npid_type_id.to_s.strip ? true : false) if hash[pid]["pids"][[@id_types[parts[1].strip], parts[0]]].nil?

              npid_set = true if valid and npid_set == false and npid.strip.length == 6 and parts[1].strip == npid_type_id.to_s.strip

            end

          end

          sids = `mysql -h #{@host} -u #{@username} -p#{@password} #{@db} -e "SELECT CONCAT(identifier, '|', identifier_type) AS identifiers FROM patient_identifier WHERE patient_id = #{sid}"` rescue nil

          if sids.nil?

            Utils::Files.log("#{@current_folder}_skips", "SKIPPED", "Skipped primary patient #{pid}:#{sid} due to MySQL query failure")

            hash.delete(pid)

            next

          end

          if sids.strip.length > 0 and sids.match(/identifier/)

            sids = sids.split("\n")

            (1..(sids.length - 1)).each do |id|

              parts = sids[id].split("|")

              if parts.length < 2

                Utils::Files.log("#{@current_folder}_minor_skips", "SKIPPED", "Skipped #{pids[id]} due to problem with IDs mapping")

                next

              end

              # Initially removed '-'s but these are usually available in ARV numbers only which are sometimes wrongly placed as National IDs
              npid = parts[0]

              valid = NationalPatientId.valid?(NationalPatientId.to_decimal(npid)) rescue false

              if valid and npid.strip.length == 6 and parts[1].strip == npid_type_id.to_s.strip

                if npid_set == false

                  hash[pid]["national_id"] = parts[0]

                else

                  hash[pid]["other_ids"][parts[0]] = (@id_types[parts[1].strip] == "National id" ? "Old Identification Number" : @id_types[parts[1].strip])

                  hash[pid]["voided"][parts[0]] = true

                end

              else

                hash[pid]["other_ids"][parts[0]] = (@id_types[parts[1].strip] == "National id" ? "Old Identification Number" : @id_types[parts[1].strip])

              end

              hash[pid]["sids"][[@id_types[parts[1].strip], parts[0]]] = (valid and npid_set == false and npid.strip.length == 6 and parts[1].strip == npid_type_id.to_s.strip ? true : false) if hash[pid]["sids"][[@id_types[parts[1].strip], parts[0]]].nil?

              npid_set = true if valid and npid_set == false and npid.strip.length == 6 and parts[1].strip == npid_type_id.to_s.strip

            end

          end

          if !hash[pid]["national_id"].nil? and hash[pid]["other_ids"].keys.length > 0

            result = RestClient.get("http://#{dde_host}:#{dde_port}/#{dde_person_db}/_design/Person/_view/by__id?key=%22#{hash[pid]["national_id"].strip}%22&reduce=false&include_docs=true") rescue nil

            if !result.nil?

              json = JSON.parse(result)["rows"][0]["doc"] rescue {}

              if json.keys.length > 0

                hash[pid]["other_ids"].each do |id, type|

                  json["patient"]["identifiers"] << {type => id} if !json["patient"]["identifiers"].include?({type => id})

                end

                file = File.open("./merges/#{@current_folder}/#{@current_folder}_#{i}.json", "a")

                file.write((j > 0 ? ",\n" : "\n") + json.to_json)

                file.close

                j += 1

                Utils::Files.log("#{@current_folder}_merged", "MERGED", " #{hash[pid]}") if !written_merged[hash[pid]]

                written_merged[hash[pid]] = true

                if j == @file_limit

                  j = 0

                  file = File.open("./merges/#{@current_folder}/#{@current_folder}_#{i}.json", "a")

                  file.write("\n]}")

                  file.close

                  i += 1

                  file = File.open("./merges/#{@current_folder}/#{@current_folder}_#{i}.json", "w")

                  file.write('{"docs":[')

                  file.close

                end
              end

            end

          else

            if !written_skips[hash[pid]]

              Utils::Files.log("#{@current_folder}_merge_skips", "SKIPPED", " #{hash[pid]}")

              written_skips[hash[pid]] = true

            end

          end

          if hash[pid]["voided"].keys.length > 0

            hash[pid]["voided"].keys.each do |id|

              result = RestClient.get("http://#{dde_host}:#{dde_port}/#{dde_person_db}/_design/Person/_view/by__id?key=%22#{id}%22&reduce=false&include_docs=true") rescue nil

              if !result.nil? and !written_voids[id]

                written_voids[id] = true

                json = JSON.parse(result)["rows"][0]["doc"] rescue {}

                if json.keys.length > 0

                  json["assigned_site"] = "???"

                  file = File.open("./merges/#{@current_folder}/#{@current_folder}_#{i}.json", "a")

                  file.write((j > 0 ? ",\n" : "\n") + json.to_json)

                  file.close

                  j += 1

                  Utils::Files.log("#{@current_folder}_merge_voids", "VOIDED", " #{id}")

                  if j == @file_limit

                    j = 0

                    file = File.open("./merges/#{@current_folder}/#{@current_folder}_#{i}.json", "a")

                    file.write("\n]}")

                    file.close

                    i += 1

                    file = File.open("./merges/#{@current_folder}/#{@current_folder}_#{i}.json", "w")

                    file.write('{"docs":[')

                    file.close

                  end

                end

              end

              result = RestClient.get("http://#{dde_host}:#{dde_port}/#{dde_npid_db}/_design/Npid/_view/by_national_id?key=%22#{id}%22&reduce=false&include_docs=true") rescue nil

              if !result.nil? and !written_npids[id]

                written_npids[id] = true

                json = JSON.parse(result)["rows"][0]["doc"] rescue {}

                if json.keys.length > 0

                  json["site_code"] = "???"

                  file = File.open("./merges/#{@current_folder}/#{@current_folder}_npids_#{i}.json", "a")

                  file.write((j > 0 ? ",\n" : "\n") + json.to_json)

                  file.close

                  j += 1

                  Utils::Files.log("#{@current_folder}_merge_npids_voids", "VOIDED", " #{id}")

                  if j == @file_limit

                    j = 0

                    file = File.open("./merges/#{@current_folder}/#{@current_folder}_npids_#{i}.json", "a")

                    file.write("\n]}")

                    file.close

                    i += 1

                    file = File.open("./merges/#{@current_folder}/#{@current_folder}_npids_#{i}.json", "w")

                    file.write('{"docs":[')

                    file.close

                  end

                end

              end

            end

          end

        end

        file = File.open("./merges/#{@current_folder}/#{@current_folder}_#{i}.json", "a")

        file.write("\n]}")

        file.close

        file = File.open("./merges/#{@current_folder}/#{@current_folder}_npids_#{i}.json", "a")

        file.write("\n]}")

        file.close

        file = File.open("#{@path.gsub(/\/$/, "")}/associations.json", "w")

        file.write(hash.to_json)

        file.close

=begin
        if File.exists?("./merges/#{@current_folder}")

          Dir.foreach("./merges/#{@current_folder}") do |file|
            next if file == '.' or file == '..'

            $stdout.write "\rUploading #{file}..."

            $stdout.flush

            # Bulk insert into CouchDB
            t = Thread.new{`curl -H "Content-Type:application/json" -X POST -d @./merges/#{@current_folder}/#{file} http://#{dde_host}:#{dde_port}/#{dde_person_db}/_bulk_docs -s > ./log/merges_#{file}.log`}

            t.join

          end

        end

        system("clear")

        puts "Done loading '#{@current_folder}' merge dumps to CouchDB..."

        sleep 1
=end

      end

    end

  end

end
  


