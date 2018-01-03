APP_ROOT = File.dirname(__FILE__)

# Array of all the folders Ruby will look in
$:.unshift(File.join(APP_ROOT, 'lib'))


require 'find'
require 'pry'
require 'pdf-reader'
require 'mongo'
require 'mediainfo-simple'
require 'dw_po_ingest'
require 'opis_po_utility'

def export_to_file(codes=[])
  File.open('POCODES.CSV', 'w') do |exportfile|
    codes.each do |code, desc|
      exportfile.puts code + "\t" + desc + "\n"
    end
  end
end

class Config
  @@options = ['1','2','3','4','5','6','QUIT','EXIT','Q','X','quit','exit','q','x']
  def self.options; @@options; end
end


def get_utility
  option = nil
  until Config.options.include?(option)
    puts "I don't understand " + option if option
    puts "1 - Export PO Codes: Strip PO Codes and description to a CSV file"
    puts "2 - Find PO Code: Searches PDFs for given PO Code"
    puts "3 - Scrape PO file for data (Make Items, PO Code, F\#'s)"
    puts "4 - Scrape PO directory for data (Make Items, PO Code, F\#'s)"
    puts "5 - Check file against the PO"
    puts "6 - PO Directory Watcher (Ingest PO Details to the database)"
    puts "quit - Quit back to the shell"
    print "> "
    user_response = gets.chomp
    cmd_line = user_response.strip.split(" ")
    option = cmd_line.shift
    #binding.pry
    args=string_to_hash(cmd_line.join(" "))
    binding.pry
  end
  return option, args
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


def get_po_code
  option = nil
  puts "Please enter a PO Code to search for, or, type 'quit' to return to the main menu: "
  print "> "
  return get_option(gets.chomp.upcase)
end

def get_option(option)
  if (option=="QUIT")||(option=='EXIT')||(option=='X')||(option=='Q')
    return :quit
  else
    return option
  end
end

def ask_for_po_number
  option = nil
  puts "Please enter a PO /# to scrape, or, type 'quit' to return to the main menu: "
  print "> "
  return get_option(gets.chomp.upcase)
end

def scrape_all
  Find.find(@source_dir) do |path|
    if FileTest.file?(path) && File.basename(path)[0] != ?.
      scrape_pdf(path)
    end
  end
end

def scrape_pdf_by_number
    ponum = nil
    until ponum==:quit
      ponum = ask_for_po_number
      binding.pry
      if ponum!=:quit
        filename="ED" + ponum.to_s + ".PDF"
        path = File.join(@source_dir, filename)
        binding.pry
        po_details=scrape_pdf(path)
        puts po_details
      end
    end
end

def scrape_pdf(file)
  if FileTest.file?(file)
    purchaseOrderHeader=true
    itemDetails = false
    opisControlNum=false
    nextLineHasPONum = false
    showTitle = ""
    detailItems=[]
    opisItems=[]
    newPO={}
    episode=""
    opisPageItem=""
    ponum=""
    reader = PDF::Reader.new(file)
    reader.pages.each do |page|
      page.to_s.each_line do |line|
        #Identify section of PO
        match=/SPECIFIC DETAILS ON ITEMS TO BE MADE\/SHIPPED ARE LISTED BELOW/.match(line)
        if match!=nil
          #puts "**********   ITEM DETAILS   ************"
          itemDetails = true
          purchaseOrderHeader = false
          next
        end
        match=/OPIS Control Number Assignment/i.match(line)
        if match!=nil
          #puts "**********   OPIS CONTROL   ************"
          opisControlNum=true
          itemDetails=false
          next
        end
        #PO Header Section
        if purchaseOrderHeader==true
          if nextLineHasPONum
            match = /\|\s*(?<ponum>D\d{6})\s*\|/.match(line)
            if match!=nil
              #puts "PONUM = " + match[:ponum]
              ponum = match[:ponum]
              nextLineHasPONum=false
            end
          end
          if (line=~/\|\s*PO#\s*\||\|\s*Ref\.#\s*\|/)!=nil
            #puts "PO Number is next"
            nextLineHasPONum = true
            next
          end
          match = /Title: (?<title>[a-zA-Z ]*)\|/i.match(line)
          if match!=nil
            showTitle = match[:title].rstrip
            #puts "Title: = " + showTitle
          end
        end
        #Item Details Section
        if itemDetails==true
          #collect items
          match = /Make Picture Item (?<item>[a-z]): (?<code>\[\d{9}\])(?<desc>.*)/i.match(line)
          if match!=nil
            #puts match[0]
            if match[:code]!='[000000000]'
              #puts "Item = " + match[:item]
              #puts "Type = " + "Picture"
              #puts "Code = " + match[:code]
              #puts "Desc = " + match[:desc].lstrip
              detailItems.push({:item=>match[:item],:type=>"Picture",:code=>match[:code],:desc=>match[:desc].lstrip})
            end
            next
          end
          match = /Make Audio Item (?<item>[a-z]): (?<code>\[\d{9}\])(?<desc>.*)/i.match(line)
          if match!=nil
            #puts match[0]
            if match[:code]!='[000000000]'
              #puts "Item = " + match[:item]
              #puts "Type = " + "Audio"
              #puts "Code = " + match[:code]
              #puts "Desc = " + match[:desc].lstrip
              detailItems.push({:item=>match[:item],:type=>"Audio",:code=>match[:code],:desc=>match[:desc].lstrip})
            end
            next
          end
        end
        #OPIS Control Section
        if opisControlNum==true
          #collect f#'s
          match = /Make Item (?<item>[a-z]):/i.match(line)
          if match!=nil
            #puts match[0]
            #puts "OPIS Item = " + match[:item]
            opisPageItem = match[:item]
            #fnumbers = []
            #detail_items[opis_page_item].merge!({:fnumbers=>fnumbers})
            next
          end
          match = /\|(?<episode>\d{4})-/.match(line)
          if match!=nil
            episode = match[:episode]
            #puts match[0]
            #puts "Episode = " + episode
          end
          match = /(?<fnumber>F\d{9})/.match(line)
          if match!=nil
            fnumber = match[:fnumber]
            #puts match[0]
            #puts "FNum = " + fnumber
            opisItems.push({:item=>opisPageItem,:episode=>episode, :fnumber=>fnumber}) if !opisItems.include?({:item=>opisPageItem,:episode=>episode, :fnumber=>fnumber})
            #if fnumbers.include?({:episode=>episode, :fnumber=>fnumber})==false
            #  fnumbers.push({:episode=>episode, :fnumber=>fnumber})
            #end
          end
        end
      end
    end
    #puts detailItems
    #puts opisItems
    newPO[:_id]=ponum
    newPO[:title]=showTitle
    newPO[:items]=detailItems
    newPO[:fnumbers]=opisItems
    #puts newPO
    return newPO
    #savePO(newPO)
  end
end

def savePO(newPO={})
  client = Mongo::Client.new(['127.0.0.1:27017'],:database=>'digital-mx')
  result = client[:purchaseOrders].insert_one(newPO)
  puts result
end

def find_po_code
  code = nil
  until code == :quit
    code = get_po_code
    if code!=:quit
      Find.find(@source_dir) do |path|
        if FileTest.file?(path) && File.basename(path)[0] != ?.
          #file_count = file_count + 1
          #progress = (file_count.to_f / total_file_count.to_f) * 100
          #print '%.2f' % progress
          #print "%\r".ljust(10)
          #$stdout.flush
          reader = PDF::Reader.new(path)
          reader.pages[0].to_s.each_line do |line|
            pocode_start = (line=~/\[\d{9}\]/)
            if pocode_start != nil
              pocode = line[/\[\d{9}\]/]
              if pocode == code
                puts path.to_s + " contains " + pocode + " = " + code
              end
            end
          end
        end
      end
    end
  end
end


def export_po_codes

  pocodes = {}
  total_file_count = 0
  file_count = 0

  Find.find(@source_dir) do |path|
    total_file_count = total_file_count + 1
  end
  Find.find(@source_dir) do |path|
    if FileTest.file?(path) && File.basename(path)[0] != ?.
      file_count = file_count + 1
      progress = (file_count.to_f / total_file_count.to_f) * 100
      print '%.2f' % progress
      print "%\r".ljust(10)
      $stdout.flush
      reader = PDF::Reader.new(path)
      #reader.pages.each do |page|
        reader.pages[0].to_s.each_line do |line|
          pocode_start = (line=~/\[\d{9}\]/)
          if pocode_start != nil
            pocode = line[/\[\d{9}\]/]
            pocode_desc = line.split(pocode)[1].strip
            if pocodes.has_key?(pocode)
              if pocodes[pocode] != pocode_desc
                pocode = pocode+file_count.to_s
              end
            end
            pocodes[pocode] = pocode_desc
          end
        end
      #end
    end
  end

  export_to_file(pocodes)

end

def buildMediaHashFromString(itemDescription)
  #strip whitespace from incoming string and convert to upper case
  itemDescription.tr!(' ','').upcase!
  #init the hash
  mediaInfo={}
  if itemDescription["MXF"]
    mediaInfo.merge!({:ext=>".MXF"})
    mediaInfo.merge!({:general_format=>"MXF"})
  elsif itemDescription["MOV"]
    mediaInfo.merge!({:ext=>".MOV"})
  elsif itemDescription["MPG"]
    mediaInfo.merge!({:ext=>".MPG"})
  elsif itemDescription["MP4"]
    mediaInfo.merge!({:ext=>".MP4"})
  end
  if itemDescription["4X3"]
      mediaInfo.merge!({:display_aspect_ratio=>"4:3"})
  elsif itemDescription["16X9"]
    mediaInfo.merge!({:display_aspect_ratio=>"16:9"})
  end
  if itemDescription["IMX"]
    mediaInfo.merge!({:general_format=>"MXF"})
    mediaInfo.merge!({:ext=>".MXF"})
    if itemDescription["IMX@50"]
      mediaInfo.merge!({:format_commercial=>"IMX 50"})
      mediaInfo.merge!({:bit_rate=>"50.0 Mbps"})
    end
    if itemDescription["IMX@30"]
      mediaInfo.merge!({:format_commercial=>"IMX 30"})
      mediaInfo.merge!({:bit_rate=>"30.0 Mbps"})
    end
  end
  if itemDescription["NTSC"]
    mediaInfo.merge!({:standard=>"NTSC"})
    mediaInfo.merge!({:frame_rate=>"29.970 fps"})
  elsif itemDescription["PAL"]
    mediaInfo.merge!({:standard=>"PAL"})
    mediaInfo.merge!({:frame_rate=>"25.000 fps"})
  end
  if itemDescription["OP1A"] || itemDescription["OPA1"]
    mediaInfo.merge!({:general_format_profile=>"OP-1a"})
  end
  if itemDescription["XDCAM"]
    mediaInfo.merge!({:format_commercial=>"XDCAM HD422"})
    if itemDescription["@50"]
      mediaInfo.merge!({:bit_rate=>"50.0 Mbps"})
    elsif itemDescription["@35"]
      mediaInfo.merge!({:format_commercial=>"XDCAM EX 35"})
      mediaInfo.merge!({:bit_rate=>"35.0 Mbps"})
    end
  end
  if itemDescription["1080I/50"] || itemDescription["1080/50I"] || itemDescription["1080I50"]
    mediaInfo.merge!({:frame_rate=>"25.000 fps"})
    mediaInfo.merge!({:display_aspect_ratio=>"16:9"})
    mediaInfo.merge!({:height=>1080})
    mediaInfo.merge!({:width=>1920})
    mediaInfo.merge!({:scan_type=>"Interlaced"})
  end
  if itemDescription["1080I/60"] || itemDescription["1080/60I"] || itemDescription["1080I60"]
    mediaInfo.merge!({:frame_rate=>"29.970 fps"})
    mediaInfo.merge!({:display_aspect_ratio=>"16:9"})
    mediaInfo.merge!({:height=>1080})
    mediaInfo.merge!({:width=>1920})
    mediaInfo.merge!({:scan_type=>"Interlaced"})
  end
  if itemDescription["1080P/24"] || itemDescription["1080/24P"] || itemDescription["1080P24"] || itemDescription["1080P/23.98"] || itemDescription["1080/23.98P"] || itemDescription["1080P23.98"]
    mediaInfo.merge!({:frame_rate=>"23.976 fps"})
    mediaInfo.merge!({:display_aspect_ratio=>"16:9"})
    mediaInfo.merge!({:height=>1080})
    mediaInfo.merge!({:width=>1920})
    mediaInfo.merge!({:scan_type=>"Progressive"})
  end
  if itemDescription["1080P/25"] || itemDescription["1080/25P"] || itemDescription["1080P25"]
    mediaInfo.merge!({:frame_rate=>"25.000 fps"})
    mediaInfo.merge!({:display_aspect_ratio=>"16:9"})
    mediaInfo.merge!({:height=>1080})
    mediaInfo.merge!({:width=>1920})
    mediaInfo.merge!({:scan_type=>"Progressive"})
  end
  if itemDescription["DVCPRO"]
    mediaInfo.merge!({:format_commercial=>"DVCPRO HD"})
  end
  match = /@(?<bitrate>\d+)MBPS/.match(itemDescription)
  if match != nil
    mediaInfo.merge!({:maximum_bit_rate=>match[:bitrate]})
  end
  if itemDescription["AVC"]
      mediaInfo.merge!({:video_format=>"AVC"})
  end
  if itemDescription["DNX175"] || itemDescription["DNXHD@175"] || itemDescription["DNXHD175"]
      mediaInfo.merge!({:format_commercial=>"DNxHD 175x"})
      mediaInfo.merge!({:video_format=>"VC-3"})
      mediaInfo.merge!({:bit_rate=>"175 Mbps"})
  end
  if itemDescription["DNX145"] || itemDescription["DNXHD@145"] || itemDescription["DNXHD145"]
      mediaInfo.merge!({:format_commercial=>"DNxHD 145x"})
      mediaInfo.merge!({:video_format=>"VC-3"})
      mediaInfo.merge!({:bit_rate=>"145 Mbps"})
  end
  if itemDescription["23.98"]
    mediaInfo.merge!({:frame_rate=>"23.976 fps"})
    mediaInfo.merge!({:scan_type=>"Progressive"})
  end
  if itemDescription["PRORES"]
    mediaInfo.merge!({:video_format=>"ProRes"})
    if itemDescription["422"]&&itemDescription["HQ"]
      mediaInfo.merge!({:video_format_profile=>"422 HQ"})
    end
  end
  if itemDescription["H.264"] || itemDescription["MOTIONJPG"] || itemDescription["MOTIONJPEG"]
    mediaInfo.merge!({:general_format=>"MPEG-4"},{:video_format=>"AVC"})
  end
  if itemDescription["MPEG-4"] || itemDescription["MPEG4"]
    mediaInfo.merge!({:general_format=>"MPEG-4"})
  end
  if itemDescription["320X240"]
    mediaInfo.merge!({:display_aspect_ratio=>"4:3", :height=>240, :width=>320})
  end
  if itemDescription["720X576"]
    mediaInfo.merge!({:height=>576, :width=>720})
  end
  if itemDescription["720X480"]
    mediaInfo.merge!({:height=>480, :width=>720})
  end
  if itemDescription["320X176"]
    mediaInfo.merge!({:height=>176, :width=>320})
  end
  if itemDescription["624X352"]
    mediaInfo.merge!({:height=>352, :width=>624})
  end
  return mediaInfo
end

def checkFileAgainstPO(file)
  #newPO[:_id]=ponum
  #newPO[:title]=showTitle
  #newPO[:items]=detailItems
  #newPO[:fnumbers]=opisItems
  po_utility = OPISPOUtility.new()
  ponum=get_ponum(file)
  poDetails={}
  mediaInfoPO={}
  item_code=""
  item_desc=""
  #filename="ED" + ponum.to_s + ".PDF"
  #path = File.join(@source_dir, filename)
  #if FileTest.file?(file) && FileTest.file?(path)
  #else
  #  puts file + " or " + path + " not found!"
  #  return
  #end
  #poDetails=scrape_pdf(path)
  po_utility.get_PO_details_db(ponum)
  poDetails=po_utility.po_details
  binding.pry
  puts "#{poDetails}"
  fnumber=get_fnum(file)
  #binding.pry
  fnumbers=poDetails[:fnumbers]
  fnumbers.each do |opis_item|
    if opis_item[:fnumber]==fnumber
      item_code = opis_item[:item]
    end
  end
  if item_code!=""
    po_items = poDetails[:items]
    po_items.each do |po_item|
      if po_item[:item]==item_code && po_item[:type]=="Picture"
        item_desc = po_item[:desc]
      end
    end
    if item_desc!=""
      binding.pry
      mediaInfoPO=buildMediaHashFromString(item_desc)
      mediaInfoFile = MediaInfo.new file
      compareMediaInfo(mediaInfoFile,mediaInfoPO)
      #binding.pry
    end
  end
end

def compareMediaInfo(file,po)
  miValue=""
  po.each do |key,value|
    case key
    when :ext
      miValue = File.extname(file.general[0].complete_name).upcase
      puts "EXT: File=" + miValue + " PO=" + value
      if miValue!=value
        puts "Invalid File Extension! MediaInfo file = " + miValue + ", PO specifies " + value
      end
    when :general_format
      miValue = file.general[0].format
      puts "G.Format: File=" + miValue + " PO=" + value
      if miValue!=value
        puts "MediaInfo.General.Format = " + miValue + ", PO specifies " + value
      end
    when :general_format_profile
      miValue = file.general[0].format_profile
      puts "G.Profile: File=" + miValue + " PO=" + value
      if miValue!=value
        puts "MediaInfo.General.Format Profile = " + miValue + ", PO specifies " + value
      end
    when :video_format
      miValue = file.video[0].format
      puts "V.format: File=" + miValue + " PO=" + value
      if miValue!=value
        puts "MediaInfo.Video.Format = " + miValue + ", PO specifies " + value
      end
    when :video_format_profile
      miValue = file.video[0].format_profile
      puts "V.profile: File=" + miValue + " PO=" + value
      if miValue!=value
        puts "MediaInfo.Video.Format Profile = " + miValue + ", PO specifies " + value
      end
    when :display_aspect_ratio
      miValue = file.video[0].display_aspect_ratio
      puts "V.aspect: File=" + miValue + " PO=" + value
      if miValue!=value
        puts "MediaInfo.Video.Aspect Ratio = " + miValue + ", PO specifies " + value
      end
    when :format_commercial
      miValue = file.video[0].format_commercial
      puts "V.Commercial: File=" + miValue + " PO=" + value
      if miValue!=value
        puts "MediaInfo.Video.Commercial Name = " + miValue + ", PO specifies " + value
      end
    when :bit_rate
      miValue = file.video[0].bit_rate
      puts "V.bitrate: File=" + miValue + " PO=" + value
      if miValue!=value
        puts "MediaInfo.Video.Bit Rate = " + miValue + ", PO specifies " + value
      end
    when :maximum_bit_rate
      miValue = file.video[0].maximum_bit_rate.to_i
      poValue = value.to_i
      puts "V.max bitrate: File=" + miValue.to_s + " PO=" + value.to_s
      if poValue>0 && miValue>poValue
        puts "MediaInfo.Video.Max Bit Rate = " + miValue + ", PO specifies " + value
      end
    when :standard
      miValue = file.video[0].standard
      puts "V.standard: File=" + miValue + " PO=" + value
      if miValue!=value
        puts "MediaInfo.Video.Standard = " + miValue + ", PO specifies " + value
      end
    when :frame_rate
      miValue = file.video[0].frame_rate
      puts "V.framerate: File=" + miValue + " PO=" + value
      if miValue!=value
        puts "MediaInfo.Video.Frame Rate = " + miValue + ", PO specifies " + value
      end
    when :height
      miValue = file.video[0].height.tr!(' ','').to_i
      puts "V.h: File=" + miValue.to_s + " PO=" + value.to_s
      if miValue!=value
        puts "MediaInfo.Video.height = " + miValue.to_s + ", PO specifies " + value.to_s
      end
    when :width
      miValue = file.video[0].width.tr!(' ','').to_i
      puts "V.w: File=" + miValue.to_s + " PO=" + value.to_s
      if miValue!=value
        puts "MediaInfo.Video.width = " + miValue.to_s + ", PO specifies " + value.to_s
      end
    when :scan_type
      miValue = file.video[0].scan_type
      puts "V.scan: File=" + miValue + " PO=" + value
      if miValue!=value
        puts "MediaInfo.Video.scan_type = " + miValue + ", PO specifies " + value
      end

    end
  end
end

def get_fnum(filename)
  fnumber=filename[/(F\d{9})/]
  if fnumber==nil
      fnumber=filename
  end
  return fnumber
end

def get_ponum(filename)
  match = /F(?<ponum>\d{6})/.match(filename)
  ponum="D" + match[:ponum]
  return ponum
end


@source_dir = "/Volumes/QX1200/PURCHASE_ORDERS"

puts "\n\n<<<< Welcome to the PO Validation Utility application >>>>\n\n"
puts "Please select a utility to continue.\n\n"

begin
  result = nil
  until result ==:quit
    # 	what do you want to do? (list, find, quit)
    #	do that action
    utility, args = get_utility
    #binding.pry
    case utility
    when '1'
      export_po_codes
    when '2'
      find_po_code
    when '3'
      scrape_pdf_by_number
    when '4'
      scrape_all
    when '5'
      binding.pry
      po_utility = OPISPOUtility.new()
      po_utility.checkFileAgainstPO(args[:file])
      puts po_utility.validation_errors
    when '6'
      po_ingest = DW_POIngest.new()
      po_ingest.launch!
    when 'QUIT','EXIT','X','Q','quit','exit','q','x'
      result=:quit
    else
      puts "\nNot a valid command.  Please pick a valid number from the list.\n"
    end
  end
rescue Exception => msg
  puts "#{msg}"
  binding.pry
  msg.backtrace.each { |line| puts "#{line}" }
  exit
end
