require "json"
require "rest-client"
require "yaml"

# Parse a JSON object

module DDE2

  class Npid

    attr :host
    attr :port
    attr :db_npids
    attr :db_person
    attr :username
    attr :password
    
    attr :current_person
    attr :current_npid
    
    attr :cursor_position
    attr :file_count
    attr :file_limit
    attr :current_folder
    
    attr :regions
    
    # Initialize the class which can be used to access selected people
    # e.g. d = DDE2::Npid.new("localhost",5984,"dde_testbench","dde_person_testbench","admin","test")
    def initialize(host, port, db_npids, db_person, username, password, file_limit=50000)
    
      @host = host
      @port = port
      @db_npids = db_npids
      @db_person = db_person
      @username = username
      @password = password
    
      @cursor_position = 0
      
      @file_count = 0
    
      @file_limit = file_limit
      
      @regions = JSON.parse(File.open("regions.json").read) rescue {}
    
    end

    # Get current state of national id
    def current_state(npid)
    
      @current_npid = nil
      
      @current_person = nil
    
      # result = JSON.parse(`curl -X GET "http://#{@host}:#{@port}/#{@db_npids}/_design/Npid/_view/by_national_id?key=%22#{npid}%22&reduce=false&include_docs=true" -s`)
      result = JSON.parse(RestClient.get("http://#{@host}:#{@port}/#{@db_npids}/_design/Npid/_view/by_national_id?key=%22#{npid}%22&reduce=false&include_docs=true")) rescue nil
    
      if !result.nil? and !result["rows"].nil? and result["rows"].length > 0
      
        @current_npid = result["rows"][0]["doc"]
      
        # person = JSON.parse(`curl -X GET "http://#{@host}:#{@port}/#{@db_person}/#{npid}" -s`)
        person = JSON.parse(RestClient.get("http://#{@host}:#{@port}/#{@db_person}/#{npid}")) rescue nil
        
        if !person.nil?
        
          @current_person = person
        
        end
      
      end
    
      if !@current_npid.nil?
      
        if (!@current_npid["site_code"].nil? and @current_npid["site_code"].strip == "???")
        
          return "in-conflict"
          
        elsif !@current_npid["site_code"].nil? and @current_npid["site_code"].strip.length > 0 and !@current_npid["assigned"].nil? and @current_npid["assigned"] == true
        
          return "assigned"
          
        elsif !@current_npid["site_code"].nil? and @current_npid["site_code"].strip.length > 0 and !@current_npid["assigned"].nil? and @current_npid["assigned"] == false
        
          return "allocated"
          
        elsif ((!@current_npid["site_code"].nil? and @current_npid["site_code"].strip.length == 0) or @current_npid["site_code"].nil?) and ((!@current_npid["assigned"].nil? and @current_npid["assigned"] == false) or @current_npid["assigned"].nil?)
        
          return "available"
          
        else
        
          return "invalid"
        
        end
      
      else

		    return "invalid"
        
      end
    
    end

    def add_update(entry, current_folder, last=false)
          
      @current_folder = current_folder   
       
      if @cursor_position == 0
              
        file = File.open("./updates/#{current_folder}/#{current_folder}_updates#{@file_count}.json", "w")
        
        file.write('{"docs":[')
        
        file.close    
      
      end
        
      file = File.open("./updates/#{current_folder}/#{current_folder}_updates#{@file_count}.json", "a")
                
      file.write((@cursor_position > 0 ? ",\n" : "\n") + entry.to_json)
      
      file.close
           
      @cursor_position += 1
      
      if @cursor_position == @file_limit
      
        @cursor_position = 0
                         
        file = File.open("./updates/#{current_folder}/#{current_folder}_updates#{@file_count}.json", "a")
        
        file.write("\n]}")
        
        file.close
          
        @file_count += 1
                     
        file = File.open("./updates/#{current_folder}/#{current_folder}_updates#{@file_count}.json", "w")
        
        file.write('{"docs":[')
        
        file.close
    
      end
      
      return true
      
    end

    def close_update_file
         
      if !@current_folder.nil?   
             
        file = File.open("./updates/#{@current_folder}/#{@current_folder}_updates#{@file_count}.json", "a")
          
        file.write("\n]}")
          
        file.close 
      
      end         
    
    end

    # Update the state of the national id in Npid document
    def update_state(npid, state, current_folder, site_code=nil, last=false)
    
      return nil if ((state.to_s.downcase == "assign" or state.to_s.downcase == "allocate") and site_code.nil?)
    
      region = @regions[site_code] rescue nil
    
      this_npid = nil
      
      this_person = nil
    
      # result = JSON.parse(`curl -X GET "http://#{@host}:#{@port}/#{@db_npids}/_design/Npid/_view/by_national_id?key=%22#{npid}%22&reduce=false&include_docs=true" -s`)
      result = JSON.parse(RestClient.get("http://#{@host}:#{@port}/#{@db_npids}/_design/Npid/_view/by_national_id?key=%22#{npid}%22&reduce=false&include_docs=true")) rescue nil
    
      if !File.exists?("./updates/#{current_folder}")
      
        Utils::Files.add_directory("./updates", "#{current_folder}")
      
      end
    
      if !result.nil? and result["rows"].length > 0
      
        this_npid = result["rows"][0]["doc"]
      
        dirty = false
        
        person = nil
        
        case state.to_s.downcase
          when "assign" then
            
            this_npid["site_code"] = site_code
            
            this_npid["assigned"] = true
            
            this_npid["region"] = region
            
            dirty = true
            
          when "allocate" then
          
            this_npid["site_code"] = site_code
            
            this_npid["assigned"] = false
            
            this_npid["region"] = region
            
            dirty = true
            
          when "in-conflict" then
          
            this_npid["site_code"] = "???"
            
            this_npid["assigned"] = nil
            
            dirty = true
            
            # person = JSON.parse(`curl -X GET "http://#{@host}:#{@port}/#{@db_person}/#{npid}" -s`)
            person = JSON.parse(RestClient.get("http://#{@host}:#{@port}/#{@db_person}/#{npid}")) rescue nil
            
            # If conflict record found, update it 
            if !person.nil?
            
              # As we can't update a primary key, we delete the current record
              # response = JSON.parse(`curl -H "Content-Type:application/json" -X DELETE "http://#{@username}:#{@password}@#{@host}:#{@port}/#{@db_person}/#{npid}\?rev\=#{person["_rev"]}" -s`)
              
              # Switched to just updating to '_delete' to allow for replication filters proper functioning
              # response = JSON.parse(RestClient.delete("http://#{@username}:#{@password}@#{@host}:#{@port}/#{@db_person}/#{npid}\?rev\=#{person["_rev"]}")) rescue nil
           
              person["_deleted"] = true
              
              JSON.parse(RestClient.post("http://#{@username}:#{@password}@#{@host}:#{@port}/#{@db_person}", person.to_json, {:content_type => :json}))
           
              # puts response
            
              # Then assign a temporary id
              person = DDE2::Npid.relegate_npid(person)
            
              person["_rev"] = nil
            
              # person = JSON.parse(`curl -H "Content-Type:application/json" -X POST -d '#{person.to_json}' "http://#{@username}:#{@password}@#{@host}:#{@port}/#{@db_person}" -s`) if !person["error"]
              
              person = JSON.parse(RestClient.post("http://#{@username}:#{@password}@#{@host}:#{@port}/#{@db_person}", person.to_json, {:content_type => :json})) if !person["error"] rescue nil
            
            end
            
        end
      
        this_npid["updated_at"] = Time.now
      
        # Only save when there are changes
        # post = JSON.parse(`curl -H "Content-Type:application/json" -X POST -d '#{this_npid.to_json}' "http://#{@username}:#{@password}@#{@host}:#{@port}/#{@db_npids}" -s`) if dirty
        # post = (JSON.parse(RestClient.post("http://#{@username}:#{@password}@#{@host}:#{@port}/#{@db_npids}", this_npid.to_json, {:content_type => :json})) rescue nil) if dirty
      
        post = add_update(this_npid, current_folder, last)
      
        # return person if state.to_s.downcase == "in-conflict"
        
        return post
      
      end
    
    end

    # Move current national id to legacy ids and assign a temporary id
    def self.relegate_npid(json)
    
      return {"error" => "invalid input"} if (json["_id"] rescue nil).nil? or (json["patient"]["identifiers"] rescue nil).nil?
      
      npid = json["_id"]
      
      tmp_id = self.generate_temporary_id
      
      json["_id"] = tmp_id

      json.delete("_id") if tmp_id.nil?

      Utils::Files.log("#{@current_folder}_deletes", "DELETED", "Deleted: #{json}") if tmp_id.nil?

      json["patient"]["identifiers"] << {"Old Identification ID" => npid}
      
      json
    
    end

    # Generate a temporary id
    def self.generate_temporary_id
      
      suffix = "%02d" % (rand * 99).round(0)
      
      base = self.convert("#{Time.now.strftime("%y%m%d%H%M%S")}#{suffix}".to_i)

      # TODO: This id algorithm seems buggy. Resorted to default GUID for the time being
      temporary_id = nil    # "0TT#{base}"
      
    end

    # Convert a Base 10 <tt>number</tt> to the specified <tt>base</tt>
    def self.convert(num)
      # we are taking out letters B, I, O, Q, S, Z because they might be
      # mistaken for 8, 1, 0, 0, 5, 2 respectively
      base_map = ['0','1','2','3','4','5','6','7','8','9','A','C','D','E','F','G',
                    'H','J','K','L','M','N','P','R','T','U','V','W','X','Y']
      base = 30
      results = ''
      quotient = num.to_i
        
      while quotient > 0 
        results = base_map[quotient % base] + results
        quotient = (quotient / base)
      end
      results
    end

    # Check if 2 json objects parsed belong to the same person
    def same_record?(existing, incoming)
    
      return false if existing.nil? or incoming.nil?
    
      if !existing["version_number"].nil? and !incoming["version_number"].nil?
      
        if existing["version_number"].strip == incoming["version_number"].strip
        
          return true
        
        end
      
      end
        
      found = [false, false, false, false]  
        
      if !existing["gender"].nil? and !incoming["gender"].nil?
      
        if existing["gender"].strip.downcase == incoming["gender"].strip.downcase
        
          found[0] = true
        
        end
      
      end  
        
      if !existing["names"]["given_name_code"].nil? and !incoming["names"]["given_name_code"].nil?
      
        if existing["names"]["given_name_code"].strip.downcase == incoming["names"]["given_name_code"].strip.downcase
        
          if found[0]
            found[1] = true
          else
            found[0] = true
          end
        
        end
      
      end  
        
      if !existing["names"]["family_name_code"].nil? and !incoming["names"]["family_name_code"].nil?
      
        if existing["names"]["family_name_code"].strip.downcase == incoming["names"]["family_name_code"].strip.downcase
        
          if found[0]
            found[1] = true
          elsif found[1]
            found[2] = true
          else
            found[0] = true
          end
        
        end
      
      end  
        
      if !existing["birthdate"].nil? and !incoming["birthdate"].nil?
      
        if existing["birthdate"].strip.downcase == incoming["birthdate"].strip.downcase
        
          if found[0]
            found[1] = true
          elsif found[1]
            found[2] = true
          else
            found[0] = true
          end
        
        end
      
      end  
       
      if !existing["patient"].nil? and !incoming["patient"].nil? and !existing["patient"]["identifiers"].nil? and !incoming["patient"]["identifiers"].nil?
      
        oldids = existing["patient"]["identifiers"] rescue []
        
        newids = incoming["patient"]["identifiers"] rescue []
        
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
        
      return true if found == [true, true, true, true]
        
      return false      
    
    end

    # Check if incoming record is more recent than current record
    def record_latest?(json)
    
      return {"error" => "invalid input"} if json["version_number"].nil? or @current_person.nil?
      
      # Get date being migrated
      json_date = json["updated_at"].match(/(\d{4})\-(\d{2})\-(\d{2})(.*)?(\d{2})\:(\d{2})\:(\d{2})/) rescue nil
      
      # Get date already saved
      current_record_date = @current_person["updated_at"].match(/(\d{4})\-(\d{2})\-(\d{2})(.*)?(\d{2})\:(\d{2})\:(\d{2})/) rescue nil
   
      if !json_date.nil? and !current_record_date.nil?
      
        # Create date
        # Sample match: "2014-08-08 16:56:10" 1:"2014" 2:"08" 3:"08" 4:" " 5:"16" 6:"56" 7:"10"
        date_json = Time.new(json_date[1].to_i, json_date[2].to_i, json_date[3].to_i, json_date[5].to_i, json_date[6].to_i, json_date[7].to_i) rescue nil
        
        date_current_record = Time.new(current_record_date[1].to_i, current_record_date[2].to_i, current_record_date[3].to_i, current_record_date[5].to_i, current_record_date[6].to_i, current_record_date[7].to_i) rescue nil
        
        # Check the dates. Parsed ecord most recent if its date is greater than saved date
        if !date_json.nil? and !date_current_record.nil?
        
          if date_json > date_current_record
          
            return true
            
          end
          
        end
      
      end
   
      return false
          
    end

    # Update verified record
    def update_record(json)
    
      return {"error" => "invalid input"} if json["version_number"].nil? or @current_person.nil?
      
      # Assign revision number to the overwriting record
      json["_rev"] = @current_person["_rev"]
      
      # Update
      # person = JSON.parse(`curl -H "Content-Type:application/json" -X POST -d '#{json.to_json}' "http://#{@username}:#{@password}@#{@host}:#{@port}/#{@db_person}" -s`)
      person = JSON.parse(RestClient.post("http://#{@username}:#{@password}@#{@host}:#{@port}/#{@db_person}", json.to_json, {:content_type => :json})) rescue nil
    
      return person
    
    end

    # Check if record belongs to the same site
    def same_site?(json)
    
      return {"error" => "invalid input"} if json["assigned_site"].nil? or @current_npid.nil?
      
      if json["assigned_site"] == @current_npid["site_code"]
      
        return true
      
      else 
      
        return false
        
      end
    
    end

  end

end
