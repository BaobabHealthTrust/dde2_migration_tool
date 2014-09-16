require "fileutils"

module Utils

  class Files
  
    def self.cleanup_directories

      if !File.exists?("./backups")

        Dir.mkdir("./backups")

      end

      if File.exists?("./data")
      
        FileUtils.mv("./data","./backups/data#{Time.now.to_s.gsub(/\s/,"_").gsub(/\:/,"_").gsub(/\+/,"_").gsub(/\-/,"_")}")

        # FileUtils.rm_rf("./data")
      
      end

      if File.exists?("./log")
      
        FileUtils.mv("./log","./backups/log#{Time.now.to_s.gsub(/\s/,"_").gsub(/\:/,"_").gsub(/\+/,"_").gsub(/\-/,"_")}")
        
      end

      if File.exists?("./dumps")
      
        FileUtils.mv("./dumps","./backups/dumps#{Time.now.to_s.gsub(/\s/,"_").gsub(/\:/,"_").gsub(/\+/,"_").gsub(/\-/,"_")}")
        
      end

      if File.exists?("./updates")
      
        FileUtils.mv("./updates","./backups/updates_dumps#{Time.now.to_s.gsub(/\s/,"_").gsub(/\:/,"_").gsub(/\+/,"_").gsub(/\-/,"_")}")
        
      end

      if File.exists?("./merges")
      
        FileUtils.mv("./merges","./backups/merges#{Time.now.to_s.gsub(/\s/,"_").gsub(/\:/,"_").gsub(/\+/,"_").gsub(/\-/,"_")}")
        
      end

    end

    def self.build_directories

      if !File.exists?("./data")
      
        Dir.mkdir("./data")
      
      end

      if !File.exists?("./log")
      
        Dir.mkdir("./log")
      
      end

      if !File.exists?("./dumps")
      
        Dir.mkdir("./dumps")
      
      end

      if !File.exists?("./updates")
      
        Dir.mkdir("./updates")
      
      end

      if !File.exists?("./reports")
      
        Dir.mkdir("./reports")
      
      end

      if !File.exists?("./backups")
      
        Dir.mkdir("./backups")
      
      end

      if !File.exists?("./merges")
      
        Dir.mkdir("./merges")
      
      end

    end

    def self.add_directory(root, folder)
    
      if File.exists?(root)
      
        root = root.strip.gsub(/\/$/,"")
        
        folder = folder.strip.gsub(/^\//,"")
      
        if !File.exists?("#{root}/#{folder}")
        
          Dir.mkdir("#{root}/#{folder}")
        
        end
      
      end
    
    end

    def self.log(site, node, message)
    
      if !File.exists?("./log/#{site}.log")
      
        file = File.open("./log/#{site}.log","w")
        
        file.write("#{Time.now} #{node}:#{message}\n")
        
        file.close
      
      else 
      
        file = File.open("./log/#{site}.log","a")
        
        file.write("#{Time.now} #{node}:#{message}\n")
        
        file.close
      
      end
    
    end

  end
  
end

