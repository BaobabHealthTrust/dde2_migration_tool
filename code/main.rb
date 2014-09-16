#!/usr/bin/env ruby
require "rubygems"
require "json"
require "yaml"
require "./bantu_soundex"
require "./dde1"
require "./national_patient_id"
require "./utils"
require "./merges"
require "./splits_and_load"

def init(migrate, filtered=false, merge_as_well=false, pre_merge_report=false)

  if migrate

    Utils::Files.cleanup_directories

    Utils::Files.build_directories

    settings = YAML.load_file("./databases.yml") rescue {}

    host = settings["mysql"]["host"]
    username = settings["mysql"]["username"]
    password = settings["mysql"]["password"]

    db_counts = {}

    (settings["target"]["databases"].split(",")).each do |db|

      dde1 = DDE1::Dump.new(host, username, password)

      dde1.load_site_data(db, filtered)

      dde = DDE1::ProcessData.new(db)

      dde.migrate_data

      db_counts[db] = {
          "inserts" => dde.new_assignments_counter,
          "updates" => dde.updates_counter,
          "allocations" => dde.allocations_counter
      }

      system("clear")

      puts "Uploading dumps into CouchDB..."

      puts

      Dir.foreach("./dumps/#{db}") do |file|
        next if file == '.' or file == '..'

        $stdout.write "\rUploading #{file}..."

        $stdout.flush

        # Bulk insert into CouchDB
        t = Thread.new { `curl -H "Content-Type:application/json" -X POST -d @./dumps/#{db}/#{file} http://#{settings["couchdb"]["host"]}:#{settings["couchdb"]["port"]}/#{settings["couchdb"]["person_database"]}/_bulk_docs -s > ./log/inserts_#{file}.log` }

        t.join

      end

      if File.exists?("./updates/#{db}")

        Dir.foreach("./updates/#{db}") do |file|
          next if file == '.' or file == '..'

          $stdout.write "\rUploading #{file}..."

          $stdout.flush

          # Bulk insert into CouchDB
          t = Thread.new { `curl -H "Content-Type:application/json" -X POST -d @./updates/#{db}/#{file} http://#{settings["couchdb"]["host"]}:#{settings["couchdb"]["port"]}/#{settings["couchdb"]["npids_database"]}/_bulk_docs -s > ./log/updates_#{file}.log` }

          t.join

        end

      end

      system("clear")

      puts "Done loading '#{db}' dumps to CouchDB..."

      sleep 1

      system("clear")

      puts "Initializing indices..."

      # Initialize indices
      `curl GET "http://#{settings["couchdb"]["host"]}:#{settings["couchdb"]["port"]}/#{settings["couchdb"]["npids_database"]}/_design/Npid/_view/all" -s`

      `curl GET "http://#{settings["couchdb"]["host"]}:#{settings["couchdb"]["port"]}/#{settings["couchdb"]["person_database"]}/_design/Person/_view/all" -s`

      system("clear")

      puts "Done indexing..."

      sleep 1

      system("clear")

    end

    system("clear")

    puts "Migration Report"
    puts "================"
    puts

    file = File.open("./reports/migration_report.txt", "w")

    file.write("Migration Report\n================\n\n")

    (settings["target"]["databases"].split(",")).each do |db|

      puts "\t#{db}:\n"

      file.write("\t#{db}:\n")

      if File.exists?("./log/#{db}_invalid.log")

        invalid = `wc -l "./log/#{db}_invalid.log"`

        invalid = /^(\d+)/.match(invalid).captures.first rescue 0

      else

        invalid = 0

      end

      if File.exists?("./log/#{db}_assigned.log")

        assigned = `wc -l "./log/#{db}_assigned.log"`

        assigned = /^(\d+)/.match(assigned).captures.first rescue 0

      else

        assigned = 0

      end

      if File.exists?("./log/#{db}_available.log")

        available = `wc -l "./log/#{db}_available.log"`

        available = /^(\d+)/.match(available).captures.first rescue 0

      else

        available = 0

      end

      if File.exists?("./log/#{db}_allocated.log")

        allocated = `wc -l "./log/#{db}_allocated.log"`

        allocated = /^(\d+)/.match(allocated).captures.first rescue 0

      else

        allocated = 0

      end

      if File.exists?("./log/#{db}_in_allocation_conflict.log")

        allocation_conflict = `wc -l "./log/#{db}_in_allocation_conflict.log"`

        allocation_conflict = /^(\d+)/.match(allocation_conflict).captures.first rescue 0

      else

        allocation_conflict = 0

      end

      if File.exists?("./log/#{db}_in_remote_conflict.log")

        remote_conflict = `wc -l "./log/#{db}_in_remote_conflict.log"`

        remote_conflict = /^(\d+)/.match(remote_conflict).captures.first rescue 0

      else

        remote_conflict = 0

      end

      if File.exists?("./log/#{db}_in_site_conflict.log")

        site_conflict = `wc -l "./log/#{db}_in_site_conflict.log"`

        site_conflict = /^(\d+)/.match(site_conflict).captures.first rescue 0

      else

        site_conflict = 0

      end

      if File.exists?("./log/#{db}_skips.log")

        skips = `wc -l "./log/#{db}_skips.log"`

        skips = /^(\d+)/.match(skips).captures.first rescue 0

      else

        skips = 0

      end

      if File.exists?("./log/#{db}_claimed_spot.log")

        claimed_spot = `wc -l "./log/#{db}_claimed_spot.log"`

        claimed_spot = /^(\d+)/.match(claimed_spot).captures.first rescue 0

      else

        claimed_spot = 0

      end

      # db_counts
      # "inserts" => dde.new_assignments_counter,
      # "updates" => dde.updates_counter,
      # "allocations"

      puts "\t\tNewly Added:\t\t\t#{db_counts[db]["inserts"]}"

      file.write("\t\tNewly Added:\t\t\t#{db_counts[db]["inserts"]}\n")

      puts "\t\tNewly Updated:\t\t\t#{db_counts[db]["updates"]}"

      file.write("\t\tNewly Updated:\t\t\t#{db_counts[db]["updates"]}\n")

      puts "\t\tNewly Allocated:\t\t#{db_counts[db]["allocations"]}"

      file.write("\t\tNewly Allocated:\t\t#{db_counts[db]["allocations"]}\n")

      puts "\t\t-------------------------------------------------------------"

      file.write("\t\t-------------------------------------------------------------\n")

      puts "\t\tAllocated:\t\t\t#{allocated}"

      file.write("\t\tAllocated:\t\t\t#{allocated}\n")

      puts "\t\tAssigned:\t\t\t#{assigned}"

      file.write("\t\tAssigned:\t\t\t#{assigned}\n")

      puts "\t\tClaimed spot:\t\t\t#{claimed_spot}"

      file.write("\t\tClaimed spot:\t\t\t#{claimed_spot}\n")

      puts "\t\tAvailable:\t\t\t#{available}"

      file.write("\t\tAvailable:\t\t\t#{available}\n")

      puts "\t\tIn-conflict:\n\t\t\tAllocation Conflict:\t#{allocation_conflict}\n\t\t\tRemote Conflict:\t#{remote_conflict}\n\t\t\tSite Conflict:\t\t#{site_conflict}\n\t\t\tTotal In-conflict:\t#{allocation_conflict.to_i + remote_conflict.to_i + site_conflict.to_i}\n\n"

      file.write("\t\tIn-conflict:\n")

      file.write("\t\t\tAllocation Conflict:\t#{allocation_conflict}\n")

      file.write("\t\t\tRemote Conflict:\t#{remote_conflict}\n")

      file.write("\t\t\tSite Conflict:\t\t#{site_conflict}\n")

      file.write("\t\t\tTotal In-conflict:\t#{allocation_conflict.to_i + remote_conflict.to_i + site_conflict.to_i}\n\n")

      puts "\t\tInvalid:\t\t\t#{invalid}"

      file.write("\t\tInvalid:\t\t\t#{invalid}\n")

      puts "\t\tSkips:\t\t\t\t#{skips}"

      file.write("\t\tSkips:\t\t\t#{skips}\n")

    end

    puts

    file.close

  end

  if merge_as_well

    # Clean all log reports
    `ls ./log/*merge*`.strip.split("\n").each{|f| FileUtils.rm(f)}

    settings = YAML.load_file("./databases.yml") rescue {}

    settings["applications"].keys.each do |site|

      host = settings["applications"][site]['host']
      db = settings["applications"][site]['database']
      username = settings["applications"][site]['username']
      password = settings["applications"][site]['password']

      dde_host = settings['couchdb']['host']
      dde_port = settings['couchdb']['port']
      dde_npid_db = settings['couchdb']['npids_database']
      dde_db = settings['couchdb']['person_database']
      dde_user = settings['couchdb']['username']
      dde_pass = settings['couchdb']['password']

      merge = Merges::Merge.new(host, username, password, db)

      merge.load_site_data(site)

      mprocess = Merges::ProcessData.new(site, host, username, password, db)

      mprocess.load_patients(dde_host, dde_port, dde_npid_db, dde_db, dde_user, dde_pass)

      if pre_merge_report == false

        system("clear")

        puts "Uploading dumps into CouchDB..."

        puts

        Dir.foreach("./merges/#{site}") do |file|
          next if file == '.' or file == '..'

          $stdout.write "\rUploading #{file}..."

          $stdout.flush

          # Bulk insert into CouchDB
          t = Thread.new { `curl -H "Content-Type:application/json" -X POST -d @./merges/#{site}/#{file} http://#{dde_host}:#{dde_port}/#{dde_db}/_bulk_docs -s > ./log/merges_#{file}.log` }

          t.join

        end

      end

    end

    if pre_merge_report

      system("clear")

      puts "Generating Pre-Merge Report..."

      puts

      counts = {}

      settings["applications"].keys.each do |site|

        if File.exists?("./data/#{site}/associations.json")

          k = 0

          json = JSON.parse(File.open("./data/#{site}/associations.json").read)

          counts[site] = {"valid" => [], "invalid" => 0, "voided" => []}

          json.keys.each do |key|

            k += 1

            $stdout.write "\rProgress: #{"% 3d" % (k * 100 / json.keys.length)}%"

            $stdout.flush

            json[key]["voided"].keys.each do |id|

              counts[site]["voided"] << id

            end

            if !json[key]["national_id"].nil?

              counts[site]["valid"] << json[key]["national_id"]

            else

              counts[site]["invalid"] += 1

            end

          end

        end

      end

      system("clear")

      puts "Pre-Merge Report"
      puts "================"
      puts

      file = File.open("./reports/pre_merge_report.txt", "w")

      file.write("Pre-Merge Report\n================\n\n")

      counts.keys.each do |key|

        puts "\t\t#{key}\n\t\t=========================================\n"

        file.write("\t\t#{key}\n\t\t=========================================\n\n")

        puts "\t\t\tValid IDs:\t#{counts[key]["valid"].length}"

        file.write("\t\t\tValid IDs:\t#{counts[key]["valid"].length}\n")

        puts "\t\t\tSkipped IDs:\t#{counts[key]["invalid"]}"

        file.write("\t\t\tSkipped IDs:\t#{counts[key]["invalid"]}\n")

        puts "\t\t\tVoided IDs:\t#{counts[key]["voided"].length}\n\n"

        file.write("\t\t\tVoided IDs:\t#{counts[key]["voided"].length}\n\n")

      end

      file.close

    else

      system("clear")

      puts "Merge Report"
      puts "================"
      puts

      file = File.open("./reports/merge_report.txt", "w")

      file.write("Merge Report\n================\n\n")

      (settings["target"]["databases"].split(",")).each do |db|

        puts "\t#{db}:\n"

        file.write("\t#{db}:\n")

        if File.exists?("./log/#{db}_merged.log")

          merged = `wc -l "./log/#{db}_merged.log"`

          merged = /^(\d+)/.match(merged).captures.first rescue 0

        else

          merged = 0

        end

        if File.exists?("./log/#{db}_merge_skips.log")

          merge_skips = `wc -l "./log/#{db}_merge_skips.log"`

          merge_skips = /^(\d+)/.match(merge_skips).captures.first rescue 0

        else

          merge_skips = 0

        end

        if File.exists?("./log/#{db}_merge_npids_voids.log")

          merge_voids = `wc -l "./log/#{db}_merge_npids_voids.log"`

          merge_voids = /^(\d+)/.match(merge_voids).captures.first rescue 0

        else

          merge_voids = 0

        end

        puts "\t\tMerged:\t\t\t\t#{merged}"

        file.write("\t\tMerged:\t\t\t\t#{merged}\n")

        puts "\t\tMerge Skips:\t\t\t#{merge_skips}"

        file.write("\t\tMerge Skips:\t\t\t#{merge_skips}\n")

        puts "\t\tMerge Voids:\t\t\t#{merge_voids}"

        file.write("\t\tMerge Voids:\t\t\t#{merge_voids}\n")

        puts "\t-------------------------------------------------------------"

        file.write("\t-------------------------------------------------------------\n")

      end

      file.close

    end

  end

end

def initialize_db

  db = Seeding::Split.new

  db.initialize_npid_data

end

def help

  puts

  puts "Migration Tool - Version 1.0, Baobab Health Trust, Sep 11 2014 Build"

  puts

  puts "Usage ./main.rb [argument]\t\tcarry out task specified by only expected argument"

  puts

  puts "Arguments:"

  puts "   -h or --help\t\t\t\tShow this message and exit"

  puts "   -f or --filter-sample\t\tFilter records to migrate using mod 10"

  puts "   -m or --migrate-only\t\t\tRun full migration only without merge data migration"

  puts "   -a or --migrate-and-merge\t\tRun full migration and merge migration back-to-back"

  puts "   -o or --merge-only\t\t\tJust run merge migration only"

  puts "   -p or --pre-merge-report-only\tJust run pre-merge migration report only"

  puts "   -i or --initialize-only\tInitialize the destination database with required seed National IDs"

  puts

end

# Options ["-h", "--help", "-f", "--filter-sample", "-m", "--migrate-only", "-a", "--migrate-and-merge", "-o", "--merge-only"]

if ARGV.length != 1

  help

else

  option = ARGV[0].strip.downcase

  case option
    when '-f'
      init(true, true, false, false)

    when '--filter-sample'
      init(true, true, false, false)

    when '-m'
      init(true, false, false, false)

    when '--migrate-only'
      init(true, false, false, false)

    when '-a'
      init(true, false, true, false)

    when '--migrate-and-merge'
      init(true, false, true, false)

    when '-o'
      init(false, false, true, false)

    when '--merge-only'
      init(false, false, true, false)

    when '-p'
      init(false, false, true, true)

    when '--pre-merge-report-only'
      init(false, false, true, true)

    when '-i'
      initialize_db

    when '--initialize-only'
      initialize_db

    else
      help

  end

end

# filtered = false

# filtered = true if ARGV.length > 0 and ARGV[0].strip.downcase == "true"

# init(filtered)
