require 'dam_files'
require 'support/string_extend'
require 'find'
require 'pry'
require 'oci8'
require 'socket'
require 'streamio-ffmpeg'

class DAMCheck

	class Config
		@@actions = ['list','find','check','move','clean','count','xdir','spaces','quit']
		@@exts_incl = ['.MOV','.MXF','.MP4','.MPG','.ZIP','.TS']
		@@exts_excl = ['.ZIP','.WAV','.AIFF']
		@@direxcl = ['NETFLIX','HULU','TAPE_OUT','BB','B_AND_B','ENTERTAIN','INSIDER']
		def self.actions; @@actions; end
		def self.exts_incl; @@exts_incl; end
		def self.exts_excl; @@exts_excl; end
		def self.direxcl; @@direxcl; end
	end

	@@dam_file_list
	@@output_file

	OPTIONS = {}

	#def self.dam_file_list; @@dam_file_list; end

	def initialize(path=nil)
		DAMFiles.filepath = path
		if DAMFiles.file_usable?
			# locate the DAM text file at path
			puts "\nDAM File found at " + path
			@@dam_file_list = DAMFiles.dam_file_list
		else
			# exit if create fails
			puts "Exiting....Could not find DAM file at " + path
			exit!
		end
		OPTIONS['find'] = [:key]
		OPTIONS['check'] = [:path, :tofile]
		OPTIONS['move'] = [:src, :dest, :commit]
		OPTIONS['clean'] = [:path, :commit, :tba, :tbd, :age, :delete_only, :nofnum]
		OPTIONS['xdir'] = [:path, :commit]
		OPTIONS['spaces'] = [:path]
	end

	def check_options(options,optdec1)
		h = options.dup
		optdec1.each do |opt|
			h.delete opt
		end
		raise ArgumentError, "no such option: #{h.keys.join(' ')}" unless h.empty?
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

	def launch!
		introduction
		# action loop
		result = nil
		until result ==:quit
			# 	what do you want to do? (list, find, quit)
			#	do that action
			action, args = get_action
			result=do_action(action, args)
		end
		conclusion
	end

	def get_action
		action = nil
		until DAMCheck::Config.actions.include?(action)
			puts "I don't understand " + action if action
			puts "Actions: " + DAMCheck::Config.actions.join(", ")
			puts "list - list all files in the DAM"
			puts "find - returns all files that match the search term provided"
			puts "check - checks the DAM for files"
			puts "move - moves files from the source to the destinaton if not in the DAM"
			puts "quit - exit the program"
			puts "clean - moves files out of the directory and stages for archive/delete"
			puts "count - returns number of files in the directory and sub-directories"
			puts "xdir - marks directories with an 'x' if it contains 0 files (i.e. empty)"
			puts "spaces - checks files and directories for leading/trailing spaces"
			print "> "
			user_response = gets.chomp
			cmd_line = user_response.strip.split(" ")
			action = cmd_line.shift
			args=string_to_hash(cmd_line.join(" "))
			#cmdline_options=string_to_hash(user_response)
			#args = user_response.strip.split(" ")
			#action = args.shift
		end
		return action, args
	end

	def do_action(action, args={})
		case action
		when 'list'
			list
		when 'find'
			check_options(args,OPTIONS['find'])
			#binding.pry
			find(args[:key])
		when 'check'
			check_options(args,OPTIONS['check'])
			check(args[:path], args[:tofile])
		when 'move'
			check_options(args,OPTIONS['move'])
			move(args[:src],args[:dest],args[:commit])
		when 'clean'
			check_options(args,OPTIONS['clean'])
			#binding.pry
			clean(args[:path],args[:age],args[:tba],args[:tbd],args[:commit],args[:delete_only],args[:nofnum])
		when 'count'
			num_files = is_dir_empty(args[:path])
			puts "#{args[:path]} has #{num_files} total files."
		when 'xdir'
			xdir(args[:path],args[:commit])
		when 'spaces'
			spaces(args[:path])
		when 'quit'
			return :quit
		else
			puts "\nNot a valid command.  Please use 'list', 'find', 'check', or 'quit'"
		end
	end

	def is_tunnel_open(host,port)
		begin
			s = TCPSocket.new host,port
			s.close
			return true
		rescue Exception => msg
			if msg.kind_of(Errno::ECONNREFUSED)
				puts "#{msg}"
				return false
			end
		end
	end

	def check_file_exact(filename)

		o = OCI8.new('diva_ro/diva_ro@lib5')
		num_rows = o.exec("select distinct E.TE_OBJECT_NAME from DIVA.dp_tape_instn_cmpt_elems e where e.te_object_name = '#{filename}'") do |r|
			#puts "#{r} exact filename is found in DAM..."
			print "!"
		end
		o.logoff
		return true if num_rows>0
		return false
	end

	def check_file_partial(partial)
		o = OCI8.new('diva_ro/diva_ro@lib5')
		num_rows = o.exec("select distinct E.TE_OBJECT_NAME from DIVA.dp_tape_instn_cmpt_elems e where e.te_object_name LIKE '%#{partial}%'") do |r|
			puts "#{r} F Number is found in the DAM..."
		end
		o.logoff
		return true if num_rows>0
		return false
	end


	def list
		output_action_header("Listing files in the DAM")
		output_file_table(@@dam_file_list)
	end

	def find(keyword="")
		output_action_header("Find #{keyword} in the DAM")
		if keyword
			#search
			files = @@dam_file_list
			found = files.select do |key,value|
				#puts "#{file.name.b}=" + file.name.b.encoding.to_s + " keyword: " + keyword.encoding.to_s
				key.include?(keyword.b.upcase)
			end
			output_file_table(found)
		else
			puts "Please use find with a file name or part of a file name, no wildcards necessary (e.g. find BLUE_BLOODS_0501_F972861000.mov or find F972861)"
		end
	end
	def spaces(source_path)
		if FileTest.directory?(source_path)
			Find.find(source_path) do |path|
				if path!=path.strip
					puts path
				else
					print "."
				end
			end
		end
	end

	def check(path,output_file)
		if FileTest.directory?(path)
			@@output_file = output_file
			check_directory(File.path(path))
		elsif FileTest.file?(path)
			check_file_exact(File.basename(path))
		else
			puts "Please use 'check' with a file or directory (e.g. check /Volumes/QX1200/ or check BLUE_BLOODS_0501_F972861000.mov"
		end
	end

	def move(source_dir, dest_dir, commit)
		if source_dir == "~"
			source_dir=File.expand_path(source_dir)
		end
		if dest_dir == "~"
			dest_dir=File.expand_path(dest_dir)
		end
		noop = !commit
		binding.pry
		if !FileTest.directory?(dest_dir)
			FileUtils.mkdir(dest_dir)
		end
		#binding.pry
		if FileTest.directory?(source_dir) && FileTest.directory?(dest_dir)
			begin
				Find.find(source_dir) do |path|
				#puts "#{path} is File = " + FileTest.file?(path).to_s
				if FileTest.directory?(path)
					if File.basename(path)[0] == ?.
						Find.prune
					else
						next
					end
				elsif FileTest.file?(path) && File.basename(path)[0] != ?.
					if DAMCheck::Config.exts_incl.include?(File.extname(path).upcase)
						if check_file_exact(File.basename(path))
							next
						else
							#binding.pry
							FileUtils.mv path, File.join(dest_dir, File.basename(path)), :verbose => true, :noop => noop
						end
					end
				else
					next
				end
			end
			rescue Exception => msg
				puts "An error of type #{msg.class} happened, message is #{msg.message}"
				#raise
			end
		end
	end

	def clean(cleanpath, age, tba, tbd, commit, delete_only,nofnum)
		source_dir = cleanpath
		fileage = age.to_i * 60 * 60 * 24
		noop = !(commit==true)
		source_dir=source_dir
		archive_dir = tba
		delete_dir = tbd
		source_dir = prep_directory(source_dir)
		archive_dir = prep_directory(archive_dir)
		delete_dir = prep_directory(delete_dir)
		binding.pry
		if FileTest.directory?(source_dir) && FileTest.directory?(archive_dir) && FileTest.directory?(delete_dir)
			begin
				Find.find(source_dir) do |path|
					#binding.pry
					if FileTest.directory?(path)
						if File.basename(path)[0] == ?. || path == archive_dir || path == delete_dir || DAMCheck::Config.direxcl.count {|x| File.basename(path).include?x } > 0
							Find.prune
						elsif (Time.now.to_i - File.mtime(path).to_i) < fileage && (Time.now.to_i - File.birthtime(path).to_i) < fileage
							Find.prune
						else
							#FileUtils.chown nil,'staff', path, :noop => noop, :verbose => true
							#FileUtils.chmod "g=rwx", path, :noop => noop, :verbose => true
							next
						end
					elsif FileTest.file?(path) && File.basename(path)[0] != ?.
						#binding.pry
						begin
							filename = clean_file_name(File.basename(path))
							fnumber=get_fnum(filename)
							if (Time.now.to_i - File.mtime(path).to_i) > fileage && (Time.now.to_i - File.birthtime(path).to_i) > fileage
								if DAMCheck::Config.exts_incl.include?(File.extname(filename).upcase)
									#binding.pry
										if ((filename =~ /(^\.|_d\.|bad|_ng\.|_ng_|_lt\.|ltsang)/i) != nil) || (((filename=~/_d_/i)!=nil) && ((filename=~/(UNDER_THE_D)/i) == nil && (filename=~/(STAR_TREK_D)/i) == nil))
											#binding.pry
											FileUtils.mv path, File.join(delete_dir, filename), :noop => noop, :verbose => true
										elsif ((filename=~ /(F\d{9})/i) != nil) || (nofnum)
											if ((filename =~ /(bw|restr|_lock_|fcp|cc|proxy)/i) != nil || (((filename=~/lock/i)!=nil) && (filename=~/matlock/i)==nil)) && !(is_prores(path))
												FileUtils.mv path, File.join(delete_dir, filename), :noop => noop, :verbose => true
											else
												in_dam = check_file_exact(filename)
												if !in_dam
													in_dam = check_file_partial(fnumber)
													if !in_dam
														if delete_only == false
															FileUtils.mv path, File.join(archive_dir, filename), :noop => noop, :verbose => true
														end
													else
														FileUtils.mv path, File.join(delete_dir, filename), :noop => noop, :verbose => true
													end
												else
													FileUtils.mv path, File.join(delete_dir, filename), :noop => noop, :verbose => true
												end
											end
											#binding.pry
										end
								elsif DAMCheck::Config.exts_excl.include?(File.extname(filename).upcase)
									next
								else
									next
								  #   binding.pry
									FileUtils.mv path, File.join(delete_dir, filename), :noop => noop, :verbose => true
								end
							end
						rescue Exception => msg
							if msg.kind_of?(Errno::EACCES)
								next
							end
							raise msg
						end
					else
						next
					end
				end
			rescue Exception => msg
				binding.pry
				puts "An error of type #{msg.class} happened, message is #{msg.message}"
				#raise
			end
		end
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


	def xdir(path, commit)
		source_dir = path
		noop = !(commit==true)
		source_dir=source_dir.upcase
		Dir.foreach(source_dir) do |dir_item|
			next if dir_item[0] == ?.
			next if dir_item[0] == "x"
			next if dir_item == "TO_BE_ARCHIVED" || dir_item == "TO_BE_DELETED"
			fullpath = File.join(source_dir,dir_item)
			puts "#{fullpath}"
			if FileTest.directory?(fullpath)
				#binding.pry
				file_count = is_dir_empty(fullpath)
				if file_count == 0
					#binding.pry
					dir_x = "x" + dir_item
					fullxpath=File.join(source_dir,dir_x)
					FileUtils.mv fullpath, fullxpath, :noop => noop, :verbose => true
				else
					puts "#{fullpath} has " + file_count.to_s + " files"
				end
			end
		end
	end

	def is_dir_empty(dir)
		file_count = 0
		Dir.foreach(dir) do |entry|
			next if entry[0] == ?.
			path = File.join(dir,entry)
			if FileTest.directory?(path)
				file_count += is_dir_empty(path)
			else
				file_count+=1
			end
		end
		return file_count
	end

	def prep_directory(dir)
		if dir[0] == "~"
			dir=File.expand_path(dir)
		end
		if !FileTest.directory?(dir)
			FileUtils.mkdir(dir)
		end
		return dir
	end

	def check_directory(dir)
		foundfiles={}
		Find.find(dir) do |path|
			#puts path
			if FileTest.directory?(path)
				#binding.pry
				puts "\n" + path
				if File.basename(path)[0] == ?.
					Find.prune
				else
					next
				end
			elsif FileTest.file?(path) && File.basename(path)[0] != ?.
				#binding.pry
				in_dam = check_file_exact(File.basename(path))
				foundfiles[path]=in_dam
				print "x"
			else
				next
			end
		end
		puts ""
		if @@output_file
			export_to_file(foundfiles)
		else
			output_file_table(foundfiles)
		end
	end

#	def check_file_partial(file)
#		#dam_files = DAMFiles.dam_file_list
#		files = @@dam_file_list
#		found = files.select do |key,value|
#			#puts "#{file.name.b}=" + file.name.b.encoding.to_s + " keyword: " + keyword.encoding.to_s
#			key.b.upcase.include?(file.b.upcase)
#		end
#		output_file_table(found)
#	end


	def introduction
		puts "\n\n<<<< Welcome to the DAM File Checker >>>>\n\n"
		puts "This is a utility to help you find files in the DAM.\n\n"
	end

	def conclusion
		puts "\n<<<< Goodbye! >>>>\n\n\n"
	end

private

	def output_action_header(text)
		puts "\n#{text.upcase.center(120)}\n\n"
	end

	def output_file_table(files={})
		print " " + "Path".ljust(80)
		print " " + "Name".ljust(40)
		print " " + "In DAM?".ljust(10) + "\n"
		puts "-" * 130
		file_count = 0
		user_response = ""
		files.each do |damfile,in_dam|
			line = " " << File.dirname(damfile).ljust(80)
			line << " " + File.basename(damfile).ljust(40)
			line << " " + in_dam.to_s.ljust(10) + "\n"
			puts line
			file_count += 1
			if file_count%10 == 0
				print "Continue listing? (type 's' to stop)"
				user_response = gets.chomp
			end
			break if user_response == 's'
		end
		puts "No listings found" if files.empty?
		puts "-" * 120
	end

	def export_to_file(files=[])
		File.open(@@output_file, 'w') do |exportfile|
			files.each do |damfile,in_dam|
				exportfile.puts File.dirname(damfile) + "\t" + File.basename(damfile) + "\t" + in_dam.to_s + "\n"
			end
		end
		return file_usable?
	end

	def file_usable?
		return false unless @@output_file
		return false unless File.exists?(@@output_file)
		return false unless File.readable?(@@output_file)
		return false unless File.writable?(@@output_file)
		return true
	end

end
