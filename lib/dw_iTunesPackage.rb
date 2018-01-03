require 'directory_watcher'
require 'pry'
require 'find'
require 'date'
require 'digest/md5'


class DW_iTunesPackage
  # watch folder script that generates file metadata and adds it to the iTunes XML metadata file
  # initialize function is called on new class instance
  # launch! function is the main entry point

  @watch_folder_path_iTunes
  OPTIONS={}

  def initialize
      # called when new instance of this class is instantiated
      # instantiate new instance of PO Utility class
      @watch_folders = {}
      @watch_folder_path_iTunes = "/Volumes/QD6000/MD5_CREATOR"
      # OPTIONS defines the command line parameters that can be used to control the watch folder process
      # dwid = id of the process
      # interval = directory watcher polling interval in seconds
      # stabel = how many intervals are required before a file is stable enough for processing. stability is defined by no change in file size.
      # start = command to start process
      # stop = command to stop the process
      # set = set parameters for process
      # quit = exit application
      OPTIONS['stop']=[:dwid]
      OPTIONS['start']=[:dwid]
      OPTIONS['set']=[:dwid, :interval, :stable]
      OPTIONS['quit']=[]
  end

  def launch!

    begin
      # setup parameters for process
      glob = ['*.mov','*.MOV']
      # startup watch folder with default parameters, process runs in a separate thread
      # see below for the process_events function which is the event delegate
      @watch_folders[:ITUNES] = DirectoryWatcher.new @watch_folder_path_iTunes, :glob => glob
      @watch_folders[:ITUNES].add_observer {|*args| args.each {|event| process_events(event,'itunes')}}
      @watch_folders[:ITUNES].interval = 15.0
      @watch_folders[:ITUNES].stable = 4
      @watch_folders[:ITUNES].persist = 'dw_itunes_package.yml'
      @watch_folders[:ITUNES].start
      # wait for input from command line
      result=nil
      until result==:quit
        # if action is not 'quit', check for valid command
        action, args = get_action
        puts "#{action}, #{args}"
  			result=do_action(action, args)
      end
      # if 'quit' stop process thread
      @watch_folders[:ITUNES].stop

    rescue Exception => msg
      # print error messages to screen and exit gracefully
      puts "#{msg}"
      msg.backtrace.each { |line| puts "#{line}" }
      @watch_folders[:ITUNES].stop
      puts "ITUNES stopped"
      exit
    end
  end

  def process_events(event,dwtype)
    begin
      if event.type==:stable
        # stable is defined by no detected file size change over the set number of intervals
        # strip file path and get the filename only
        filename = File.basename(event.path)
        # print status to screen
        puts "#{dwtype}: #{Time.now.to_s} - #{event.type}.... #{event.path}"
        puts "Generating md5 checksum for #{filename}..."
        md5 = Digest::MD5.file(event.path).hexdigest
        puts "md5 checksum for #{filename} is #{md5}"
        # create md5 file
        md5File = File.join(@watch_folder_path_iTunes,filename + ".txt")
        File.open(md5File, 'w') do |txtfile|
          txtfile.puts "#{md5}"
    		end
      else
        # if not stable, print status to screen
        puts "#{dwtype}: #{Time.now.to_s} - #{event.type}.... #{event.path}"
      end
    rescue Exception => msg
      puts "#{msg}"
      msg.backtrace.each { |line| puts "#{line}" }
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
    # check if command entered is a valid command
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
    # set new parameters for the watch folder
    puts "dwid = #{dwid} interval = #{interval} stable = #{stable}"
    if !dwid.nil?
      # valid process id required first
      if @watch_folders.has_key?(dwid.to_sym)
        # stop the process and set the new parameters
        stop_watch_folder(dwid)
        # set the interval
        if !interval.nil?
          if interval.to_i < 1
            puts "Interval must be greater than 0. Setting to default value (15)."
            interval = 15
          end
          @watch_folders[dwid.to_sym].interval=interval.to_i
        end
        # set the number of intervals needed for file stability
        if stable.nil?
          #ignore this setting
        elsif stable=="nil"
          puts "WARNING: Setting stable to nil will turn off stable events from processing. No files will be moved until the value for stable is greater than 0."
          @watch_folders[dwid.to_sym].stable = nil
        elsif stable.to_i < 1
          puts "The value for stable must be greater than 0. Setting to default value (15)."
          @watch_folders[dwid.to_sym].stable = stable.to_i
        end
        # start process with new parameters
        start_watch_folder(dwid)
      else
        puts "The watch folder with ID# #{dwid} does not exist. Available watch folder id's are: #{@watch_folders.keys}"
      end
    end
  end

  def stop_watch_folder(dwid)
    # stop the watch folder process, if process id is provided then stop that specific one.
    # if no id, then stop all processes
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
    # start the watch folder process, if process id is provided then start that specific one.
    # if no id, then start all processes
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
end
