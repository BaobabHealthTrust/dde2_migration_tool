require "yaml"
require "./utils"
require "./bantu_soundex"
require "./dde2"

module DDE1

  class Dump
  
    attr :host
    attr :db
    attr :username
    attr :password       
    
    def initialize(host, username, password)
    
      @host = host
      @username = username
      @password = password
    
    end
  
    def dump_data(table, folder, filtered=false)
    
      system("clear")
    
      puts "Dumping table '#{table}' for database '#{folder}'..."
    
      Utils::Files.add_directory("./data", folder)
    
      if table == "legacy_national_ids"
    
        `mysql -h #{@host} -u #{@username} -p#{@password} #{db} -e "SELECT CONCAT(person_id, '|', value) as value FROM legacy_national_ids" > ./data/#{folder}/legacies.sql`
    
      elsif table == "national_patient_identifiers"
    
        `mysql -h #{@host} -u #{@username} -p#{@password} #{db} -e "SELECT CONCAT(value, '|', assigner_site_id) as value FROM national_patient_identifiers WHERE COALESCE(person_id,'') = ''#{(filtered ? " AND (id%10) = 0" : "")}" > ./data/#{folder}/allocated.sql`
    
        `mysql -h #{@host} -u #{@username} -p#{@password} #{db} -e "SELECT CONCAT(person_id, '|', value, '|', assigner_site_id) as value FROM national_patient_identifiers" > ./data/#{folder}/assigned.sql`
    
        `mysql -h #{@host} -u #{@username} -p#{@password} #{db} -e "SELECT CONCAT(id, '|', code) as site FROM sites" > ./data/#{folder}/sites.sql`
    
      elsif table == "people"
      
        `mysql -h #{@host} -u #{@username} -p#{@password} #{db} -e "SELECT CONCAT(id,'|',data,'|',created_at,'|',updated_at,'|',COALESCE(version_number,''),'|',given_name,'|',family_name,'|',COALESCE(gender,''),'|',COALESCE(birthdate,'1900-01-01'),'|',birthdate_estimated,'|',creator_site_id) as value FROM people#{(filtered ? " WHERE (id%10) = 0" : "")}" > ./data/#{folder}/people.sql`
      
      end
    
    end
  
    def load_site_data(db, filtered=false)
          
      @db = db
    
      tables = ["people", "national_patient_identifiers", "legacy_national_ids"]
      
      tables.each do |table|
      
        dump_data(table, db, filtered)
      
      end
    
      system("clear")
    
      puts "Done dumping data..."
    
    end
  
  end

  class ProcessData
  
    attr :identifiers
    attr :allocated
    attr :npids
    attr :valid
    attr :path
    attr :sites
    attr :current_folder
    attr :file_limit
    
    # DDE2 Connection parameters
    attr :host
    attr :port
    attr :db_npids
    attr :db_person
    attr :username
    attr :password
    
    attr :dde2_updates
    
    attr :allocations_counter
    attr :new_assignments_counter
    attr :updates_counter
    
    def initialize(folder, file_limit=50000)
    
      if !File.exists?("./data/#{folder}")
        
        @valid = false
      
        return {"error" => "Target source does not exist!"}
      
      end
    
      @path = "./data/#{folder}"
    
      @identifiers = {}
      @sites = {}
      @allocated = {}
    
      @current_folder = folder
    
      @file_limit = file_limit
    
      @valid = true
      
      settings = YAML.load_file("./databases.yml")["couchdb"] rescue {}
            
      @host = settings["host"]
      @port = settings["port"]
      @db_npids = settings["npids_database"]
      @db_person = settings["person_database"]
      @username = settings["username"]
      @password = settings["password"]
    
      @dde2_updates = DDE2::Npid.new(@host, @port, @db_npids, @db_person, @username, @password)        
    
      @allocations_counter = 0
      @new_assignments_counter = 0
      @updates_counter = 0
      
    end  
    
    def extract_values(person)
    
      data = person.split("|")
      
=begin      
       [
          "1",           
          "{\"addresses\":{\"state_province\":\"Lilongwe\",\"city_village\":\"Mtema1\",\"county_district\":\"Mtema\",\"address2\":\"Lilongwe\",\"address1\":\"Mtema\",\"neighborhood_cell\":\"Other\"},\"names\":{\"family_name\":\"\",\"given_name\":\"\"},\"patient\":{\"identifiers\":{\"old_identification_number\":\"\"}},\"gender\":\"M\",\"attributes\":{\"race\":\"\",\"occupation\":\"\",\"citizenship\":\"\",\"cell_phone_number\":\"\"},\"birthdate_estimated\":\"1\",\"birthdate\":\"1949-07-15\"}",           
          "2013-03-29 14:21:13",           
          "2013-03-29 14:21:13", 
          "23454100-abd0-da16-81bb-e4c9bd7d1bf7", 
          "", 
          "", 
          "M", 
          "1949-07-15", 
          "1",
          4]      
=end
      
      return nil if data.length < 7
      
      # Seed Object
      json = JSON.parse(data[1]) rescue nil
      
      return nil if json.nil?
      
      json["person_id"] = data[0]
      
      json["created_at"] = data[2]
      
      json["updated_at"] = data[3]
            
      json["version_number"] = data[4]
      
      json["type"] = "Person"
      
      json["names"]["given_name_code"] = json["names"]["given_name"].soundex rescue nil
      
      json["names"]["family_name_code"] = json["names"]["family_name"].soundex rescue nil
      
      json["birthdate"] = data[8]

      json["birthdate_estimated"] = (json["birthdate_estimated"].to_s.strip == '1' ? true : false)

      json["addresses"]["home_district"] = (json["addresses"]["address2"] rescue nil)

      json["addresses"].delete("address2") rescue nil

      json["addresses"]["home_ta"] = (json["addresses"]["county_district"] rescue nil)

      json["addresses"].delete("county_district") rescue nil

      json["addresses"]["home_village"] = (json["addresses"]["neighborhood_cell"] rescue nil)

      json["addresses"].delete("neighborhood_cell") rescue nil

      json["addresses"]["current_district"] = (json["addresses"]["state_province"] rescue nil)

      json["addresses"].delete("state_province") rescue nil

      json["addresses"]["current_ta"] = (json["addresses"]["township_division"] rescue nil)

      json["addresses"].delete("township_division") rescue nil

      json["addresses"]["current_village"] = (json["addresses"]["city_village"] rescue nil)

      json["addresses"].delete("city_village") rescue nil

      json["addresses"]["current_residence"] = (json["addresses"]["address1"] rescue nil)

      json["addresses"].delete("address1") rescue nil

                 
      # Load site id in 'people' table in case we don't have one in 'npids' table           
      json["assigned_site"] = @sites[data[10].to_s.strip] # rescue nil
         
      Utils::Files.log("#{@current_folder}_site_codes", "SITE CODE", "#{json["person_id"]} : #{json["assigned_site"] } : #{data[10].to_s}")       
                    
      ids = json["patient"]["identifiers"] rescue {}
      
      collection = []
      
      ids.keys.each do |key|
      
        collection << {key => ids[key]}
      
      end
      
      json["patient"]["identifiers"] = collection
    
      if !json["attributes"].nil?
      
        json["person_attributes"] = json["attributes"]
        
        json.delete("attributes")
      
      end
    
      return json
    
    end
    
    # Main entry point
    def migrate_data
    
      system("clear")
      
      puts "Loading assigned '#{@current_folder}' people data..."
    
      load_assigned_npids
      load_legacy_ids
               
      if File.exists?("#{@path.gsub(/\/$/,"")}/people.sql")
    
        if !File.exists?("./dumps/#{@current_folder}")
        
          Utils::Files.add_directory("./dumps", "#{@current_folder}")
        
        end
    
        i = 0
        j = 0
        k = 0
        
        file = File.open("./dumps/#{@current_folder}/#{@current_folder}_#{i}.json", "w")
        
        file.write('{"docs":[')
        
        file.close
    
        lines = File.open("#{@path.gsub(/\/$/,"")}/people.sql").readlines
        
        puts
        
        lines.each do |person|
        
          k += 1
        
          $stdout.write "\rProgress: #{"% 3d" % (k * 100 / lines.length)}%" 
          
          $stdout.flush
        
          json = extract_values(person)
        
          if json.nil? or @identifiers[json["person_id"]].nil?
          
            Utils::Files.log("#{@current_folder}_skips", "SKIPS", "Skipped record with #{person}") if !person.strip.match(/^value/)
          
            next 
          
          end
        
          if !json["person_id"].nil?
          
            # Append legacy IDs
            json["patient"] = {} if json["patient"].nil?
            
            json["patient"]["identifiers"] = [] if json["patient"]["identifiers"].nil?
            
            if !@identifiers[json["person_id"]].nil? and !@identifiers[json["person_id"]]["legacies"].nil?
            
              @identifiers[json["person_id"]]["legacies"].each do |id|
              
                json["patient"]["identifiers"] << id
                
              end
            
            end
          
            Utils::Files.log("#{@current_folder}_migration", "INSERT", "Loading person with ID #{json["person_id"]}")
                  
            dde2 = DDE2::Npid.new(@host, @port, @db_npids, @db_person, @username, @password)
                             
            # Associate NPID                 
            npid = @identifiers[json["person_id"]]["assigned"]["npid"] rescue nil

            # json["assigned_site"] = (@identifiers[json["person_id"]]["assigned"]["site"] rescue nil) if !(@identifiers[json["person_id"]]["assigned"]["site"] rescue nil ).nil?    
        
            if !npid.nil?       
               
              json["_id"] = npid
                   
              json["assigned_site"] = @identifiers[json["person_id"]]["assigned"]["site"] rescue nil if !(@identifiers[json["person_id"]]["assigned"]["site"] rescue nil ).nil?    
                   
              state = dde2.current_state(npid.strip)
              
              # Log initial state
              Utils::Files.log("#{@current_folder}_states", "INITIAL STATE", "#{npid}: #{state}")
                     
              cansave = true       
                      
              if state.strip.downcase == "assigned"
              
                # Log state
                Utils::Files.log("#{@current_folder}_assigned", "ASSIGNED", "#{npid}: #{state}")
                 
                same_site = dde2.same_site?(json) rescue false
              
                if same_site
                
                  same_person = dde2.same_record?(dde2.current_person, json)
                
                  Utils::Files.log("#{@current_folder}_same_record_check", "SAME RECORD", "Hit check for person with npid #{npid}")
    
                  if same_person
                  
                    record_latest = dde2.record_latest?(json)
                    
                    if record_latest
                    
                       dde2.update_record(json)
                       
                       # Log state
                       Utils::Files.log("#{@current_folder}_updated", "UPDATED", "#{npid}: #{state}")
                 
                       @updates_counter += 1
                 
                    end
                  
                    cansave = false
                    
                  else
                  
                    @dde2_updates.update_state(npid, "in-conflict", @current_folder)
                  
                    # Log state
                    Utils::Files.log("#{@current_folder}_in_site_conflict", "CONFLICT", "#{npid}: in-conflict - same site")
                 
                    json = DDE2::Npid.relegate_npid(json)
                  
                  end
                
                else
                
                    @dde2_updates.update_state(npid, "in-conflict", @current_folder)
                  
                    # Log state
                    Utils::Files.log("#{@current_folder}_in_remote_conflict", "CONFLICT", "#{npid}: in-conflict - different site")
                 
                    json = DDE2::Npid.relegate_npid(json)                  
                
                end
              
              elsif state.strip.downcase == "allocated"
                              
                same_site = dde2.same_site?(json) rescue false
              
                if same_site
                
                  @dde2_updates.update_state(npid, "assign", @current_folder, json["assigned_site"])
                
                  # Log state
                  Utils::Files.log("#{@current_folder}_claimed_spot", "ASSIGNED ALLOCATED SPOT", "#{npid}: #{state}")
                 
                  # json = DDE2::Npid.relegate_npid(json)                                  
                
                else                
                
                  @dde2_updates.update_state(npid, "in-conflict", @current_folder)
                  
                  # Log state
                  Utils::Files.log("#{@current_folder}_in_allocation_conflict", "CONFLICT", "#{npid}: in-conflict - by allocation")
                 
                  json = DDE2::Npid.relegate_npid(json)                                  
                
                end
              
              elsif state.strip.downcase == "available"
                                        
                  @dde2_updates.update_state(npid, "assign", @current_folder, json["assigned_site"])                      
                            
                  # Log state
                  Utils::Files.log("#{@current_folder}_available", "ASSIGNED", "#{npid}: #{state}")
                 
              elsif state.strip.downcase == "in-conflict"
              
                  @dde2_updates.update_state(npid, "in-conflict", @current_folder)
                  
                  # Log initial state
                  Utils::Files.log("#{@current_folder}_in_allocation_conflict", "CONFLICT", "#{npid}: in-conflict - by allocation")
               
                  json = DDE2::Npid.relegate_npid(json)                                                 
                       
              else
              
                  # Log initial state
                  Utils::Files.log("#{@current_folder}_invalid", "INVALID", "#{npid}: invalid NPID")
               
                  json = DDE2::Npid.relegate_npid(json)                                  
                                         
              end
                   
              if cansave   
                             
                file = File.open("./dumps/#{@current_folder}/#{@current_folder}_#{i}.json", "a")
                
                file.write((j > 0 ? ",\n" : "\n") + json.to_json)
                
                file.close
                    
                @new_assignments_counter += 1
                   
                j += 1
                
                if j == @file_limit
                
                  j = 0
                                   
                  file = File.open("./dumps/#{@current_folder}/#{@current_folder}_#{i}.json", "a")
                  
                  file.write("\n]}")
                  
                  file.close
                    
                  i += 1
                               
                  file = File.open("./dumps/#{@current_folder}/#{@current_folder}_#{i}.json", "w")
                  
                  file.write('{"docs":[')
                  
                  file.close
              
                end
              
              end
              
            else
            
              cansave = false
            
              ids = []
              
              if !json["patient"].nil? and !json["patient"]["identifiers"].nil?
              
                json["patient"]["identifiers"].each do |id|
                
                  ids << id[id.keys[0]] if !id[id.keys[0]].to_s.length == 0
                
                end
              
              end
            
              Utils::Files.log("#{@current_folder}_same_record_check", "SAME RECORD", "Hit method with #{json}")
    
              response = RestClient.post("http://#{@host}:#{@port}/#{@db_person}/_design/Person/_view/search_by_all_identifiers?reduce=false&include_docs=true&limit=10", {"keys" => ids}.to_json, {:content_type => :json}) rescue nil
            
              matches = []
              
              (JSON.parse(response)["rows"] rescue []).each do |pmatch|
              
                matches << pmatch["doc"]
              
              end
            
              found = nil
              
              matches.each do |existing|
              
                if !existing["patient"].nil? and !json["patient"].nil? and !existing["patient"]["identifiers"].nil? and !json["patient"]["identifiers"].nil?
              
                  oldids = existing["patient"]["identifiers"] rescue []
                  
                  newids = json["patient"]["identifiers"] rescue []
                  
                  oldids.each do |id|
                  
                    break if found[3]
                  
                    newids.each do |nid|
                    
                      if id[id.keys[0]].strip == nid[nid.keys[0]].strip
                      
                        found[3] = true
                        
                        break
                        
                      end
                    
                    end
                  
                  end
                
                end 
              
              end
            
              if found.nil?
                
                tmp_id = DDE2::Npid.generate_temporary_id
                
                json["_id"] = tmp_id

                json.delete("_id") if tmp_id.nil?

                Utils::Files.log("#{@current_folder}_deletes", "DELETED", "Deleted: #{json}") if tmp_id.nil?

                file = File.open("./dumps/#{@current_folder}/#{@current_folder}_#{i}.json", "a")
                
                file.write((j > 0 ? ",\n" : "\n") + json.to_json)
                
                file.close
                    
                @new_assignments_counter += 1
                   
                j += 1
                
                if j == @file_limit
                
                  j = 0
                                   
                  file = File.open("./dumps/#{@current_folder}/#{@current_folder}_#{i}.json", "a")
                  
                  file.write("\n]}")
                  
                  file.close
                    
                  i += 1
                               
                  file = File.open("./dumps/#{@current_folder}/#{@current_folder}_#{i}.json", "w")
                  
                  file.write('{"docs":[')
                  
                  file.close
              
                end
                            
                Utils::Files.log("#{@current_folder}_invalid", "INVALID", "Person without 'NPID' assigned temporary id:\t #{json}")
              
              else
              
                Utils::Files.log("#{@current_folder}_skips", "SKIPS", "Skipped person without 'NPID' with an existing match:\t #{json}")
              
              end
              
            end
        
          else
          
            Utils::Files.log("#{@current_folder}_errors", "ERROR", "Loading person failed without 'person_id':\n\t #{json}")
          
          end
           
        end
    
        file = File.open("./dumps/#{@current_folder}/#{@current_folder}_#{i}.json", "a")
        
        file.write("\n]}")
        
        file.close
    
      end
         
      # Clean hashes
      @identifiers = {}
      @allocated = {}      
        
      puts
           
      system("clear")
       
      puts "Done loading assigned '#{@current_folder}' people data..."
          
      sleep 1
            
      system("clear")
          
      puts "Loading allocated '#{@current_folder}' npids data..."
    
      puts
    
      # Load assignments      
      load_allocated
    
      k = 0
   
      @allocated.each do |npid, site|
      
        k += 1
        
        $stdout.write "\rProgress: #{"% 3d" % (k * 100 / @allocated.keys.length)}%" 
          
        $stdout.flush
        
        # t = Thread.new{
        
        update_npid(npid, site, "allocate", (k == @allocated.keys.length - 1 ? true : false)) if site.strip.length > 0
        
        # }
        
        # t.join
      
      end
 
      @dde2_updates.close_update_file
 
      @allocated = {}   
      
      system("clear")
      
      puts "Done loading allocated '#{@current_folder}' npids data..."
        
    end
    
    def update_npid(npid, site, state, last)    
      
        dde2 = DDE2::Npid.new(@host, @port, @db_npids, @db_person, @username, @password)
        
        current_state = dde2.current_state(npid)
        
        if current_state.strip.downcase == "available"
        
          @dde2_updates.update_state(npid, state, @current_folder, site, last)
        
          Utils::Files.log("#{@current_folder}_allocated", "ALLOCATED", "Allocated #{npid}:#{site}")
          
          @allocations_counter += 1
        
        end      
    
    end
    
    def load_allocated
    
      if File.exists?("#{@path.gsub(/\/$/,"")}/sites.sql")
      
        rows = File.open("#{@path.gsub(/\/$/,"")}/sites.sql").readlines
        
        rows.each do |row|
        
          values = row.split("|")
          
          if values.length == 2
          
            @sites[values[0].strip] = values[1].strip
          
          end
          
        end
      
      end
    
      if File.exists?("#{@path.gsub(/\/$/,"")}/allocated.sql")
      
        rows = File.open("#{@path.gsub(/\/$/,"")}/allocated.sql").readlines
        
        rows.each do |row|
        
          values = row.split("|")
          
          if values.length == 2
          
            @allocated[values[0].strip] = @sites[values[1].strip]
          
          end
        
        end
      
      end
    
    end
  
    def load_legacy_ids
    
      if File.exists?("#{@path.gsub(/\/$/,"")}/legacies.sql")
      
        rows = File.open("#{@path.gsub(/\/$/,"")}/legacies.sql").readlines
        
        rows.each do |row|
        
          values = row.split("|")
          
          if values.length == 2
          
            if @identifiers[values[0].strip].nil?
            
              @identifiers[values[0].strip] = {
                "legacies" => [
                  {"Old Identification ID" => values[1].strip}
                ]
              }
            
            elsif @identifiers[values[0].strip]["legacies"].nil?
            
              @identifiers[values[0].strip]["legacies"] = [
                {"Old Identification ID" => values[1].strip}
              ]
            
            else
            
              @identifiers[values[0].strip]["legacies"] << {"Old Identification ID" => values[1].strip}
            
            end
          
          end
        
        end
      
      end
    
    end
  
    def load_assigned_npids
    
      if File.exists?("#{@path.gsub(/\/$/,"")}/sites.sql")
      
        rows = File.open("#{@path.gsub(/\/$/,"")}/sites.sql").readlines
        
        rows.each do |row|
        
          values = row.split("|")
          
          if values.length == 2
          
            @sites[values[0].to_s.strip] = values[1].strip
          
          end
          
        end
      
      end
    
      if File.exists?("#{@path.gsub(/\/$/,"")}/assigned.sql")
      
        rows = File.open("#{@path.gsub(/\/$/,"")}/assigned.sql").readlines
        
        rows.each do |row|
        
          values = row.split("|")
          
          if values.length == 3
          
            if @identifiers[values[0].strip].nil?
            
              @identifiers[values[0].strip] = {
                  "assigned" => {
                    "npid" => values[1].strip,                  
                    "site" => @sites[values[2].strip]
                  }                
                }
            
            elsif @identifiers[values[0].strip]["assigned"].nil?
            
               @identifiers[values[0].strip]["assigned"] = {
                  "npid" => values[1].strip,                  
                  "site" => @sites[values[2].strip]
                }             
            
            else
            
              @identifiers[values[0].strip]["assigned"] = {
                  "npid" => values[1].strip,                  
                  "site" => @sites[values[2].strip]
                }             
            
            end
          
          end
        
        end
      
      end
    
    end
  
  end

end
