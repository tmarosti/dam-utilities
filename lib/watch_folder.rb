require 'directory_watcher'
require 'pry'
require 'dam_check'
require 'find'
require 'date'
require 'streamio-ffmpeg'
require 'oci8'
require 'socket'
require 'opis_po_utility'

class DAMDirectoryWatcher
  #@dam_check
  @watch_folder_path
  @archive_path
  @delete_path
  @noop
  @is_tunnel_open
  @db_connection
  @watch_folders
  @po_utility
  OPTIONS={}

  def initialize(noop=true)
    #@dam_check = DAMCheck.new('DAMFileList.txt')
    @watch_folder_path_1200 = '/Volumes/QX1200/TO_BE_ARCHIVED'
    @review_folder_path_1200 = '/Volumes/QX1200/TO_BE_ARCHIVED/REVIEW'
    @watch_folder_path_6000 = '/Volumes/QD6000/PRODUCTION/TO_BE_ARCHIVED'
    @review_folder_path_6000 = '/Volumes/QD6000/PRODUCTION/TO_BE_ARCHIVED/REVIEW'
    @watch_folder_path_archive = '/Volumes/QX1200/TAPE_ARCHIVE/IN'
    @archive_path_1200 = '/Volumes/QX1200/TAPE_ARCHIVE/DMX_IN'
    @delete_path_1200 = '/Volumes/QX1200/TO_BE_DELETED'
    @delete_path_6000 = '/Volumes/QD6000/PRODUCTION/TO_BE_DELETED'
    @bb_archive_path = '/Volumes/QX1200/TAPE_ARCHIVE/BB_IN'
    @watch_folder_path_diva = '/Volumes/QX1200/TAPE_ARCHIVE/DIVA'
    @noop = noop
    @watch_folders = {}
    OPTIONS['stop']=[:dwid]
    OPTIONS['start']=[:dwid]
    OPTIONS['set']=[:dwid, :interval, :stable]
    OPTIONS['quit']=[]
  end

  def launch!
    begin
      # only watch for files with these extensions
      glob = ['*.mov','*.mxf','*.mp4','*.mpg','*.zip','*.ts','*.tar','*.aif','*.MOV','*.MXF','*.MP4','*.MPG','*.ZIP','*.TS','*.TAR','*.AIF']

      # build dictionary of watch folder objects threads, key is the process id
      @watch_folders[:QX1200] = DirectoryWatcher.new @watch_folder_path_1200, :glob => glob
      @watch_folders[:QD6000] = DirectoryWatcher.new @watch_folder_path_6000, :glob => glob
      @watch_folders[:archive] = DirectoryWatcher.new @watch_folder_path_archive, :glob => glob
      @watch_folders[:diva] = DirectoryWatcher.new @watch_folder_path_diva, :glob => glob

      @watch_folders[:QX1200].add_observer {|*args| args.each {|event| process_events(event,'1200')}}
      @watch_folders[:QD6000].add_observer {|*args| args.each {|event| process_events(event,'6000')}}
      @watch_folders[:archive].add_observer {|*args| args.each {|event| process_events(event,'archive')}}
      @watch_folders[:diva].add_observer {|*args| args.each {|event| process_events(event,'diva')}}

      @watch_folders[:QX1200].interval = 15.0
      @watch_folders[:QX1200].stable = 20
      @watch_folders[:QX1200].persist = 'wf_1200.yml'
      #@watch_folders[:QX1200].min_age = 0  # = 7 days

      @watch_folders[:QX1200].start

      @watch_folders[:QD6000].interval = 15.0
      @watch_folders[:QD6000].stable = 20
      @watch_folders[:QD6000].persist = 'wf_6000.yml'
      #@watch_folders[:QD6000].min_age = 0  # = 7 days
      @watch_folders[:QD6000].start

      @watch_folders[:archive].interval = 15.0
      @watch_folders[:archive].stable = 20
      @watch_folders[:archive].persist = 'wf_archive.yml'
      #@watch_folders[:archive].min_age = 0  # = 7 days
      @watch_folders[:archive].start

      @watch_folders[:diva].interval = 15.0
      @watch_folders[:diva].stable = 20
      @watch_folders[:diva].persist = 'wf_diva.yml'
      #@watch_folders[:archive].min_age = 0  # = 7 days
      @watch_folders[:diva].start

      result=nil
      until result==:quit
        action, args = get_action
        puts "#{action}, #{args}"
  			result=do_action(action, args)
      end

      @watch_folders[:QX1200].stop
      @watch_folders[:QD6000].stop
      @watch_folders[:archive].stop
      @watch_folders[:diva].stop

    rescue Exception => msg
      puts "#{msg}"
      msg.backtrace.each { |line| puts "#{line}" }
      @watch_folders[:QX1200].stop
      puts "1200 stopped"
      @watch_folders[:QD6000].stop
      puts "6000 stopped"
      @watch_folders[:archive].stop
      puts "archive stopped"
      @watch_folders[:diva].stop
      puts "diva stopped"
      exit
    end

  end

  def check_options(options,optdec1)
		optdec1.each do |opt|
      if !options.key?(opt)
        options[opt]=nil
      end
		end
	end

  def get_action
		user_response = gets.chomp
		cmd_line = user_response.strip.split(" ")
		action = cmd_line.shift
		args=string_to_hash(cmd_line.join(" "))
		#cmdline_options=string_to_hash(user_response)
		#args = user_response.strip.split(" ")
		#action = args.shift
		return action, args
	end

  def string_to_hash(hashstring)
    # function converts string to hash keys
    hash_to_return={}
		hashstring.split(':').each do |pair|
			next if pair.length==0
			#binding.pry
			key,value = pair.split("=>")
			key.strip!
			key.delete! ":"
			value.strip!
			value = true if value =~ (/^(true|t|yes|y)$/i)
			value = false if value =~ (/^(false|f|no|n)$/i)
			hash_to_return[key.to_sym] = value
		end
		return hash_to_return
	end

  def do_action(action, args={})

    check_options(args,OPTIONS[action])
		case action
  		when 'stop'
        stop_watch_folder(args[:dwid])
  		when 'start'
        start_watch_folder(args[:dwid])
  		when 'set'
        set_watch_folder(args[:dwid], args[:interval], args[:stable])
  		when 'quit'
  			return :quit
  		else
  			puts "\nNot a valid command.  Please use 'stop', 'start', 'set', or 'quit'"
		end
	end

  def set_watch_folder(dwid, interval, stable)
    puts "dwid = #{dwid} interval = #{interval} stable = #{stable}"
    if !dwid.nil?
      if @watch_folders.has_key?(dwid.to_sym)
        stop_watch_folder(dwid)
        if !interval.nil?
          if interval.to_i < 1
            puts "Interval must be greater than 0. Setting to default value (15)."
            interval = 15
          end
          @watch_folders[dwid.to_sym].interval=interval.to_i
        end
        if stable.nil?
          #ignore this setting
        elsif stable=="nil"
          puts "WARNING: Setting stable to nil will turn off stable events from processing. No files will be moved until the value for stable is greater than 0."
          @watch_folders[dwid.to_sym].stable = nil
        elsif stable.to_i < 1
          puts "The value for stable must be greater than 0. Setting to default value (15)."
          @watch_folders[dwid.to_sym].stable = stable.to_i
        end
        start_watch_folder(dwid)
      else
        puts "The watch folder with ID# #{dwid} does not exist. Available watch folder id's are: #{@watch_folders.keys}"
      end
    end
  end

  def stop_watch_folder(dwid)
    if !dwid.nil?
      if @watch_folders[dwid.to_sym].running? then
        @watch_folders[dwid.to_sym].stop
        puts "stopping #{@watch_folders[dwid.to_sym].config.dir}..."
        while @watch_folders[dwid.to_sym].running?
          print "."
        end
        print "stopped\n"
      else
        puts "#{dwid} watch folder is stopped"
      end
    else
      @watch_folders.each do |key,wf|
        if wf.running?
          wf.stop
          puts "stopping #{wf.config.dir}..."
          while wf.running?
            print "."
          end
          print "stopped\n"
        else
          puts "#{wf.config.dir} stopped"
        end
      end
    end
  end

  def start_watch_folder(dwid)
    if !dwid.nil?
      if !@watch_folders[dwid.to_sym].running? then
        @watch_folders[dwid.to_sym].start
        puts "starting #{@watch_folders[dwid.to_sym].config.dir}..."
        while !@watch_folders[dwid.to_sym].running?
          print "."
        end
        print "started\n"
      else
        puts "#{@watch_folders[dwid.to_sym].config.dir} watch folder is running"
      end
    else
      @watch_folders.each do |key,wf|
        if !wf.running?
          wf.start
          puts "starting #{wf.config.dir}..."
          while !wf.running?
            print "."
          end
          print "started\n"
        else
          puts "#{wf.config.dir} started"
        end
      end
    end
  end

  def process_events(event,dwtype)
    begin
      if event.type==:stable
        if !is_tunnel_open('127.0.0.1',1521)
          puts "The SSH Tunnel Connection to the DIVA database is not open.  Please enable the tunnel (type 'fptunnel' at a terminal prompt) and restart the watch folders."
          stop_watch_folder('1200')
          stop_watch_folder('6000')
          stop_watch_folder('archive')
          stop_watch_folder('diva')
          return
        end
        filename = clean_file_name(File.basename(event.path))
        fnumber=get_fnum(filename)

        if dwtype=='1200'
          archive_path = @watch_folder_path_archive
          delete_path = @delete_path_1200
          review_path = @review_folder_path_1200
          #add_to_watch_files_1200(fnumber,event.path)
        elsif dwtype=='6000'
          archive_path = @watch_folder_path_archive
          delete_path = @delete_path_6000
          review_path = @review_folder_path_6000
          #add_to_watch_files_6000(fnumber,event.path)
        elsif dwtype=='archive'
          archive_path = @watch_folder_path_archive
          delete_path = @delete_path_1200
          review_path = @review_folder_path_1200
          #add_to_watch_files_archive(fnumber,event.path)
        elsif dwtype=='diva'
          archive_path = @watch_folder_path_archive
          delete_path = @delete_path_1200
          review_path = @review_folder_path_1200
        end
        if ((filename =~ /(^\.|_d\.|bad|_ng\.|_ng_|delete)/i) != nil) || (((filename=~/_d_/i)!=nil) && ((filename=~/(UNDER_THE_D)/i) == nil && (filename=~/(STAR_TREK_D)/i) == nil))
          #do not archive files with .*, *_d.*, *_d_*, *_ng.*, *_ng_*, *proxy*, or *.wav
          puts "#{dwtype}: #{Time.now.to_s} - Invalid files for Archive, moving to #{delete_path}"
          new_path = File.join(delete_path, filename)
  				FileUtils.mv event.path, new_path, :noop => @noop, :verbose => true
        elsif filename =~ /(^BB_|_BB_)/i
          print "#{dwtype}: #{Time.now.to_s} - Checking DAM for #{filename}"
          in_dam = check_file_exact(filename)
          puts ".... #{in_dam}"
          if !in_dam
            #Move BB files to archive path
            puts "#{dwtype}: #{Time.now.to_s} - B & B file...Moving to #{@bb_archive_path}"
            #new_path = File.join(bb_archive_path, filename)
            FileUtils.mv event.path, File.join(@bb_archive_path, filename), :noop => @noop, :verbose => true
          else
            puts "#{dwtype}: #{Time.now.to_s} - Already in the DAM, moving to #{delete_path}"
            new_path = File.join(delete_path, filename)
            FileUtils.mv event.path, File.join(delete_path, filename), :noop => @noop, :verbose => true
          end
        elsif filename =~ /(F\d{9})/i
          #File has a properly structured F#, check if in the DAM
          if ((filename =~ /(bw|restr|_lock_|fcp|cc-ref|restricted)/i) != nil || (((filename=~/lock/i)!=nil) && (filename=~/matlock/i)==nil))
            puts "#{dwtype}: #{Time.now.to_s} - #{event.path} is a proxy, moving to #{delete_path}"
            new_path = File.join(delete_path, filename)
            FileUtils.mv event.path, new_path, :noop => @noop, :verbose => true
          else
            if (File.extname(filename).upcase!=".ZIP") && (dwtype != "diva")
              print "#{dwtype}: #{Time.now.to_s} - Checking #{filename} against the PO Specifications...."
              @po_utility=OPISPOUtility.new() if @po_utility==nil
              @po_utility.checkFileAgainstPO(event.path)
              # if there are differences in the file from the PO, create an error file named the same as the media file
              # put both the error file and the media file in the REVIEW folder for manual review
              if @po_utility.validation_errors.count > 0
                puts "#{dwtype}: #{Time.now.to_s} - File does not match the PO Specifications, requires review...Moving to #{review_path}"
                new_path = File.join(review_path, filename)
                FileUtils.mv event.path, File.join(review_path, filename), :noop => @noop, :verbose => true
                error_file_path = File.join(review_path,filename + ".error.log")
                export_error_file(@po_utility.validation_errors,error_file_path)
                return
              end
            end
            print "#{dwtype}: #{Time.now.to_s} - Checking DAM for #{filename}"
            in_dam = check_file_exact(filename)
            puts ".... #{in_dam}"
            if !in_dam
                #Move to Archive
                print "#{dwtype}: #{Time.now.to_s} - Checking DAM for #{fnumber}"
                in_dam = check_file_fnum(fnumber)
                puts ".... #{in_dam}"
                if !in_dam
                  if dwtype == '6000' || dwtype == '1200'
                    puts "#{dwtype}: #{Time.now.to_s} - File not in the DAM...moving to #{archive_path}"
                    new_path = File.join(archive_path, filename)
                    FileUtils.mv event.path, File.join(archive_path, filename), :noop => @noop, :verbose => true
                  elsif (dwtype == 'archive') || (dwtype == "diva")
                    puts "#{dwtype}: #{Time.now.to_s} - File not in the DAM...Saving metadata"
                    @po_utility=OPISPOUtility.new() if @po_utility==nil
                    @po_utility.saveAssetMetadata(event.path)
                  end
              else
                  puts "#{dwtype}: #{Time.now.to_s} - Duplicate F-Number, requires review...Moving to #{review_path}"
                  new_path = File.join(review_path, filename)
                  FileUtils.mv event.path, File.join(review_path, filename), :noop => @noop, :verbose => true
                end
            else
              puts "#{dwtype}: #{Time.now.to_s} - Already in the DAM, moving to #{delete_path}"
              new_path = File.join(delete_path, filename)
              FileUtils.mv event.path, File.join(delete_path, filename), :noop => @noop, :verbose => true
            end
          end
        else
          puts "#{dwtype}: #{Time.now.to_s} - #{event.type}.... #{event.path}"
        end
        ##########
      #elsif event.type==:removed
        #fnumber=get_fnum(File.basename(event.path))
        #if dwtype=='1200'
        #  remove_from_watch_files_1200(fnumber,event.path)
        #elsif dwtype=='6000'
        #  remove_from_watch_files_6000(fnumber,event.path)
        #elsif dwtype=='archive'
        #  remove_from_watch_files_archive(fnumber,event.path)
        #else
        #end
      else
        puts "#{dwtype}: #{Time.now.to_s} - #{event.type}.... #{event.path}"
      end
    rescue Exception => msg
      puts "#{msg}"
      msg.backtrace.each { |line| puts "#{line}" }
      exit
    end
  end

  def export_error_file(errors=[],error_file_path)
		File.open(error_file_path, 'w') do |errorfile|
			errors.each do |key,value|
				errorfile.puts "#{key}: #{value}" + "\r\n"
			end
		end
	end


  def is_tunnel_open(host,port)
    begin
      s = TCPSocket.new host,port
      s.close
      return true
    rescue Exception => msg
      if msg.kind_of?(Errno::ECONNREFUSED)
        puts "#{msg}"
        return false
      end
      puts "#{msg}"
      msg.backtrace.each { |line| puts "#{line}" }
    end
  end

  def check_file_exact(filename)

    o = OCI8.new('diva_ro/diva_ro@lib5')
    num_rows = o.exec("select distinct E.TE_OBJECT_NAME from DIVA.dp_tape_instn_cmpt_elems e where e.te_object_name = '#{filename}'") do |r|
      puts "#{r} exact filename is found in DAM..."
    end
    o.logoff
    return true if num_rows>0
    return false
  end

  def check_file_fnum(fnumber)
    o = OCI8.new('diva_ro/diva_ro@lib5')
    num_rows = o.exec("select distinct E.TE_OBJECT_NAME from DIVA.dp_tape_instn_cmpt_elems e where e.te_object_name LIKE '%#{fnumber}%'") do |r|
      puts "#{r} F Number is found in the DAM..."
    end
    o.logoff
    return true if num_rows>0
    return false
  end


  def clean_file_name(filename)
    clean_file_name = filename
    #strip leading and trailing spaces
    clean_file_name.strip!
    #change all spaces to _
    clean_file_name.sub!(" ","_")
    extname = File.extname(clean_file_name)
    basename = File.basename(clean_file_name,".*")
    until !basename.end_with?(".")
      basename.chomp!(".")
    end
    clean_file_name = basename + extname
    return clean_file_name
  end

  def is_prores(file_pwd)
    vidfile = FFMPEG::Movie.new(file_pwd)
    return vidfile.video_codec.include?('prores')
  end

  def get_fnum(filename)
    fnumber=filename[/(F\d{9})/]
    if fnumber==nil
        fnumber=filename
    end
    return fnumber
  end
end
