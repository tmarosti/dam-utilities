require 'find'
require 'pry'
require 'pdf-reader'
require 'mongo'
require 'mediainfo-simple'
require 'digest/md5'



class OPISPOUtility
  # this class is used to extract file specifications from the PO PDF files
  # relies heavily on the format of the PO being the same.  Any change to the format will invalidate this code
  # set class properties
  attr_accessor :source_dir
  attr_accessor :po_details
  attr_accessor :mongo_db_ip
  attr_accessor :mongo_db_po_collection
  attr_accessor :mongo_db_errorLog_collection
  attr_accessor :mongo_db_port
  attr_accessor :mongo_db_database
  attr_accessor :validation_errors
  attr_accessor :mongo_db_asset_collection

  # class level variables
  @mongo_db_connection

  def initialize
    # this function is called on new instance of the class
    # set class level variables
    @source_dir = "/Volumes/QX1200/PURCHASE_ORDERS"
    @mongo_db_ip = "127.0.0.1"
    @mongo_db_port = 27017
    @mongo_db_database = "digital-mx"
    @mongo_db_po_collection = "purchaseOrders"
    @mongo_db_errorLog_collection = "fileErrorLog"
    @mongo_db_asset_collection = "mediaAssets"
  end

  def scrape_all_in_dir(save_to_db)
    # function to re-process all PDF files in the directory
    # parameters:
    # save_to_db = boolean to save the data to the database. if false, data is printed to the screen only
    raise ArgumentError, "set the 'source_dir' property (got nil)" if @source_dir == nil
    Find.find(@source_dir) do |path|
      if FileTest.file?(path) && File.basename(path)[0] != ?.
        puts "Scraping..." + path
        scrape_pdf_by_file(path,save_to_db)
      end
    end
  end

  def scrape_pdf_by_number(po_num,save_to_db)
    # function that processes specific PDF file by PO#
    # po_num = PO#
    # save_to_db = boolean to save the data to the database. if false, data is printed to the screen only
    raise ArgumentError, "give the PO/# as a parameter (got nil)" if po_num == nil
    filename="ED" + po_num.to_s + ".PDF"
    path = File.join(@source_dir, filename)
    scrape_pdf_by_file(path,save_to_db)
  end

  def scrape_pdf_by_file(file,save_to_db)
    # function that processes specific PDF file by file name
    # file = file to process (includes full path)
    # save_to_db = boolean to save the data to the database. if false, data is printed to the screen only
    raise ArgumentError, "give the file as a parameter (got nil)" if file == nil
    raise ArgumentError, "#{file} does not exist" if ! File.exist? file
    @po_details = scrape_pdf(file)
    if save_to_db
      puts "Saving..." + file
      savePO
    end
  end

  def scrape_pdf(file)
    # extract file specifications for the given file, uses RegEx to identify the data in the file
    # file = file to process (includes full path)
    raise ArgumentError, "give the file as a parameter (got nil)" if file == nil
    raise ArgumentError, "#{file} does not exist" if ! File.exist? file
    # is file actually a file?
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
      # instantiate new PDF Reader class passing the filename with path
      reader = PDF::Reader.new(file)
      reader.pages.each do |page|
        #binding.pry can be used here to pause the script and check variables for debug purposes
        page.to_s.each_line do |line|
          #Identify section of PO
          # ITEM DETAILS section
          match=/SPECIFIC DETAILS ON ITEMS TO BE MADE\/SHIPPED ARE LISTED BELOW/.match(line)
          if match!=nil
            #puts "**********   ITEM DETAILS   ************"
            itemDetails = true
            purchaseOrderHeader = false
            next
          end
          # OPIS Control Section
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
            # PO Number is here
            if (line=~/\|\s*PO#\s*\||\|\s*Ref\.#\s*\|/)!=nil
              #puts "PO Number is next"
              nextLineHasPONum = true
              next
            end
            # Show Title
            match = /Title: (?<title>[a-zA-Z /d]*)\|/i.match(line)
            if match!=nil
              showTitle = match[:title].rstrip
              #puts "Title: = " + showTitle
            end
          end
          #Item Details Section
          if itemDetails==true
            #collect items
            # Video Specs are here, make array of all Video assets to be made for this PO
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
            # Audio Specs are here, make array of all Audio assets to be made for this PO
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
            #binding.pry
            #collect f#'s
            match = /Make Item (?<item>[a-z]):/i.match(line)
            if match!=nil
              #binding.pry
              #puts match[0]
              #puts "OPIS Item = " + match[:item]
              opisPageItem = match[:item]
              #fnumbers = []
              #detail_items[opis_page_item].merge!({:fnumbers=>fnumbers})
              next
            end
            # episode numbers have multiple patterns
            match = /\|(?<episode>\d{4}-\d{1})/.match(line)
            if match==nil
              #binding.pry
              match = /\|(?<episode>\d{4})/.match(line)
              if match==nil
                match = /\|(?<episode>S\d{3})/.match(line)
                if match==nil
                  match = /\|(?<episode>\d{3})/.match(line)
                end
              end
            end
            if match!=nil
              episode = match[:episode]
              puts "Episode = " + episode
            end
            # get F# and add Page, episode#, and F# to opisItems array if not already exist
            match = /(?<fnumber>F\d{9})/.match(line)
            if match!=nil
              #binding.pry
              fnumber = match[:fnumber]
              #puts match[0]
              #puts "FNum = " + fnumber
              opisItems.push({:item=>opisPageItem,:episode=>episode, :fnumber=>fnumber}) if ! opisItems.include?({:item=>opisPageItem,:episode=>episode, :fnumber=>fnumber})
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

  def validate_data
    # is the PDF a properly formatted PO?
    errors=[]
    errors.push "No PO details!" if @po_details.empty?
    errors.push "Oops! no PO number" if ! @po_details.has_key?(:_id)
    errors.push "Oops! no PO items specified" if ! @po_details.has_key?(:items)
    errors.push "Oops! there needs to be at least 1 PO item" if @po_details[:items].count < 1
    errors.push "Oops! no PO F\#'s specified" if ! @po_details.has_key?(:fnumbers)
    errors.push "Oops! there needs to be at least 1 F\#" if @po_details[:fnumbers].count < 1
    if errors.count > 0
      puts errors
      return false
    end
    return true
  end


  def savePO()
    # save the specs to database.  This data is used to validate files before archive into DIVA
    raise ArgumentError, "set the 'mongo_db_collection' property (got nil)" if @mongo_db_po_collection == nil
    # if the PO is valid format
    if validate_data
      open_mongo_db_connection
      doc = @mongo_db_connection[@mongo_db_po_collection].find(:_id=>@po_details[:_id])
      #binding.pry
      if doc.count==0
        # if PO does not exist in DB
        doc = @mongo_db_connection[@mongo_db_po_collection].insert_one(@po_details)
      else
        #binding.pry
        # if PO exists, delete and insert
        result = @mongo_db_connection[@mongo_db_po_collection].delete_one(:_id=>@po_details[:_id])
        if result.n > 0
          doc = @mongo_db_connection[@mongo_db_po_collection].insert_one(@po_details)
        end
      end
      #return result
    end
  end

  def saveErrorLog(file)
    raise ArgumentError, "set the 'mongo_db_collection' property (got nil)" if @mongo_db_errorLog_collection == nil
    filename= File.basename(file)
    @validation_errors[:_id]=filename
    @validation_errors[:datetime]=DateTime.now.strftime "%m/%d/%Y %I:%M:%S.%L"
    @validation_errors[:fileLocation]=File.dirname(file)
    open_mongo_db_connection
    doc = @mongo_db_connection[@mongo_db_errorLog_collection].find(:_id=>filename)
    #binding.pry
    if doc.count==0
      doc = @mongo_db_connection[@mongo_db_errorLog_collection].insert_one(@validation_errors)
    else
      #binding.pry
      result = @mongo_db_connection[@mongo_db_errorLog_collection].delete_one(:_id=>filename)
      if result.n > 0
        doc = @mongo_db_connection[@mongo_db_errorLog_collection].insert_one(@validation_errors)
      end
    end
  end

  def saveAsset(assetData)
    raise ArgumentError, "set the 'mongo_db_collection' property (got nil)" if @mongo_db_asset_collection == nil
    open_mongo_db_connection
    filename = assetData["filename"]
    raise ArgumentError, "filename is required" if filename == nil
    doc = @mongo_db_connection[@mongo_db_asset_collection].find(:filename=>filename)
    #binding.pry
    if doc.count==0
      doc = @mongo_db_connection[@mongo_db_asset_collection].insert_one(assetData)
    else
      #binding.pry
      result = @mongo_db_connection[@mongo_db_asset_collection].delete_many(:filename=>filename)
      if result.n > 0
        doc = @mongo_db_connection[@mongo_db_asset_collection].insert_one(assetData)
      end
    end
  end

  def open_mongo_db_connection
    if @mongo_db_connection==nil
      raise ArgumentError, "set the 'mongo_db_ip' property (got nil)" if @mongo_db_ip == nil
      raise ArgumentError, "set the 'mongo_db_port' property (got nil)" if @mongo_db_port == nil
      raise ArgumentError, "set the 'mongo_db_database' property (got nil)" if @mongo_db_database == nil
      connection_string = @mongo_db_ip + ":" + @mongo_db_port.to_s
      @mongo_db_connection = Mongo::Client.new([connection_string], :database=>@mongo_db_database)
    end
  end

  def get_PO_details_db(po_number)
    open_mongo_db_connection
    @po_details = @mongo_db_connection[@mongo_db_po_collection].find(:_id=>po_number).first
  end

  def buildMediaHashFromString(itemDescription)
    # takes PO Item Description and builds a list of fields and values expected from MediaInfo for a file built using the same specifications
    # itemDescription = the line item string from the PDF PO that is the specification requirements
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
      #mediaInfo.merge!({:general_format=>"MXF"})
      #mediaInfo.merge!({:ext=>".MXF"})
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
        mediaInfo.merge!({:format_commercial=>"XDCAM HD 35"})
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
      #if itemDescription["1080"]
      #  mediaInfo.merge!({:width=>1280})
      #end
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
        mediaInfo.merge!({:format_commercial=>"DNxHD 145"})
        mediaInfo.merge!({:video_format=>"VC-3"})
        mediaInfo.merge!({:bit_rate=>"145 Mbps"})
    end
    if itemDescription["23.98"]
      mediaInfo.merge!({:frame_rate=>"23.976 fps"})
      mediaInfo.merge!({:scan_type=>"Progressive"})
    end
    if itemDescription["PRORES"]
      mediaInfo.merge!({:video_format=>"ProRes"})
      #if itemDescription["422"]
      #  if itemDescription["HQ"]
      #    mediaInfo.merge!({:video_format_profile=>"422 HQ"})
      #  else
      #    mediaInfo.merge!({:video_format_profile=>"422"})
      #  end
      #end
    end
    if itemDescription["H.264"] || itemDescription["MOTIONJPG"] || itemDescription["MOTIONJPEG"]
      mediaInfo.merge!({:general_format=>"MPEG-4"})
      mediaInfo.merge!({:video_format=>"AVC"})
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
    # file = full path filename for the PO file in PDF format, 
    #newPO[:_id]=ponum
    #newPO[:title]=showTitle
    #newPO[:items]=detailItems
    #newPO[:fnumbers]=opisItems
    @validation_errors={}
    @po_details={}
    # set new instance of MediaInfo class, pass media file to be analyzed with full path
    mediaInfoFile = MediaInfo.new file
    return if !mediaInfoFile.video?
    filename= File.basename(file)
    fnumber=filename[/(F\d{9})/]
    # if file does not have a F# or the PO has not been received yet, let the file be and just return
    if fnumber==nil
      puts = "Invalid Filename Error: #{file} does not have a valid F\#."
      #@validation_errors[:invalid_filename] = "Invalid Filename Error: #{file} does not have a valid F\#."
      return
    end
    ponum=get_ponum(fnumber)
    # get PO Details from the DB
    get_PO_details_db(ponum)
    if @po_details==nil
      puts "Missing PO: There is no Purchase Order on file for PO\# #{ponum}."
      #@validation_errors[:missing_po] = "Missing PO: There is no Purchase Order on file for PO\# #{ponum}."
      return
    end
    match = /_(?<episode>\d{4}-\d{1})_/.match(filename)
    if match==nil
      #binding.pry
      match = /_(?<episode>\d{4})_/.match(filename)
      if match==nil
        match = /_(?<episode>S\d{3})_/.match(filename)
        if match==nil
          match = /_(?<episode>\d{3})_/.match(filename)
          if match==nil
            match = /_(?<episode>\d{4}-\d{1})R_/.match(filename)
            if match==nil
              #binding.pry
              match = /_(?<episode>\d{4})R_/.match(filename)
              if match==nil
                match = /_(?<episode>S\d{3})R_/.match(filename)
                if match==nil
                  match = /_(?<episode>\d{3})R_/.match(filename)
                end
              end
            end
          end
        end
      end
    end
    if match!=nil
      episode = match[:episode]
      puts "Episode = " + episode
    else
      episode=""
      puts "No episode match #{filename}"
    end


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
    #binding.pry
    # Use the F# and and Episode to retrieve the Item Code
    fnumbers=@po_details[:fnumbers]
    fnumbers.each do |opis_item|
      if opis_item[:fnumber]==fnumber
        if opis_item[:episode]==episode
          item_code = opis_item[:item]
          break
        end
      end
    end
    # Use the Item Code to get the File Specification (Item Desc) for Video Assets
    # Audio Assets can't be analyzed yet
    if item_code!=""
      po_items = @po_details[:items]
      po_items.each do |po_item|
        if po_item[:item]==item_code && po_item[:type]=="Picture"
          item_desc = po_item[:desc]
          break
        end
      end
      # Use the Item Desc and build and array of expected MediaInfo values for a file formatted based on the specs
      # comapre the MediaInfo values for the file against the MediaHash array and create an array of differences (@validation_errors)
      if item_desc!=""
        #binding.pry
        mediaInfoPO=buildMediaHashFromString(item_desc)
        compareMediaInfo(mediaInfoFile,mediaInfoPO)
        #binding.pry
      else
        @validation_errors[:invalid_po]="Invalid PO #{ponum}: There are no Picture items for Make Item #{item_code}."
      end
    else
      @validation_errors[:invalid_fnumber]="Invalid F\#: F\# #{fnumber} is not found on the PO #{ponum}."
    end
    if @validation_errors.count > 0
      # if there are differences in the file from the PO, save to the DB
      saveErrorLog(file)
    end
  end

  def compareMediaInfo(file,po)
    # file = MediaInfo class object
    # po = array of expected MediaInfo fields and values based on the Item Description in the PO
    miValue=""
    po.each do |key,value|
      case key
      when :ext
        miValue = File.extname(file.general[0].complete_name).upcase
        puts "EXT: File=#{miValue} PO=#{value}"
        @validation_errors[:ext]="Invalid File Extension! MediaInfo file = #{miValue}, PO specifies #{value}" if miValue!=value
      when :general_format
        miValue = file.general[0].format
        puts "G.Format: File=#{miValue} PO=#{value}"
        @validation_errors[:general_format]="Format Error (MediaInfo=>General=>Format): File Format = #{miValue}, PO specifies #{value}" if miValue!=value
      when :general_format_profile
        miValue = file.general[0].format_profile
        puts "G.Profile: File=#{miValue} PO=#{value}"
        @validation_errors[:general_format_profile]="Format Profile Error (MediaInfo=>General=>Format Profile):  File Profile = #{miValue}, PO specifies #{value}" if miValue!=value
      when :video_format
        miValue = file.video[0].format
        puts "V.format: File=#{miValue} PO=#{value}"
        @validation_errors[:video_format]="Video Format Error (MediaInfo=>Video=>Format): File Video Format = #{miValue}, PO specifies #{value}" if miValue!=value
      when :video_format_profile
        miValue = file.video[0].format_profile
        puts "V.profile: File=#{miValue} PO=#{value}"
        @validation_errors[:video_format_profile]="Video Format Profile Error (MediaInfo=>Video=>Format Profile): File Video Profile = #{miValue}, PO specifies #{value}" if miValue!=value
      when :display_aspect_ratio
        miValue = file.video[0].display_aspect_ratio
        puts "V.aspect: File=#{miValue} PO=#{value}"
        @validation_errors[:display_aspect_ratio]="Display Aspect Ratio Error (MediaInfo=>Video=>Aspect Ratio): File Aspect Ratio = #{miValue}, PO specifies #{value}" if miValue!=value
      when :format_commercial
        miValue = file.video[0].format_commercial
        puts "V.Commercial: File=#{miValue} PO=#{value}"
        @validation_errors[:format_commercial]="Commercial Name Error (MediaInfo=>Video=>Commercial Name): File Commercial Name =  #{miValue}, PO specifies #{value}" if miValue!=value
      when :bit_rate
        miValue = file.video[0].bit_rate
        puts "V.bitrate: File=#{miValue} PO=#{value}"
        @validation_errors[:bit_rate]="Bit Rate Error (MediaInfo=>Video=>Bit Rate): File Bit Rate =  #{miValue}, PO specifies #{value}" if miValue.to_i.ceil!=value.to_i.ceil
      when :maximum_bit_rate
        miValue = file.video[0].maximum_bit_rate.to_i
        poValue = value.to_i
        puts "V.max bitrate: File=#{miValue} PO=#{value}"
        @validation_errors[:maximum_bit_rate]="Maximum Bit Rate Error (MediaInfo=>Video=>Max Bit Rate): File Max Bit Rate =  #{miValue.to_s}, PO specifies #{poValue.to_s}" if miValue.to_i.ceil>poValue.to_i.ceil
      when :standard
        miValue = file.video[0].standard
        puts "V.standard: File=#{miValue} PO=#{value}"
        @validation_errors[:standard]="Standard Error (MediaInfo=>Video=>Standard): File Standard =  #{miValue}, PO specifies #{value}" if miValue!=value
      when :frame_rate
        miValue = file.video[0].frame_rate
        puts "V.framerate: File=#{miValue} PO=#{value}"
        @validation_errors[:frame_rate]="Frame Rate Error (MediaInfo=>Video=>Frame Rate): File Frame Rate =  #{miValue}, PO specifies #{value}" if miValue!=value
      when :height
        miValue = file.video[0].height.tr!(' ','').to_i
        puts "V.h: File=#{miValue} PO=#{value}"
        @validation_errors[:height]="Height Error (MediaInfo=>Video=>Height): File Height =  #{miValue.to_s}, PO specifies #{value.to_s}" if miValue!=value
      when :width
        miValue = file.video[0].width.tr!(' ','').to_i
        puts "V.w: File=#{miValue} PO=#{value}"
        @validation_errors[:width]="Width Error (MediaInfo=>Video=>Width): File Width =  #{miValue.to_s}, PO specifies #{value.to_s}" if miValue!=value
      when :scan_type
        miValue = file.video[0].scan_type
        miValue = "Interlaced" if miValue=="MBAFF"
        puts "V.scan: File=#{miValue} PO=#{value}"
        @validation_errors[:scan_type]="Scan Type Error (MediaInfo=>Video=>Scan): File Scan Type =  #{miValue}, PO specifies #{value}" if miValue!=value
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
    match = /F(?<ponumber>\d{6})/.match(filename)
    retVal="D" + match[:ponumber]
    return retVal
  end

  def saveAssetMetadata(file)
    mediaFile = {}
    generalHash = {}
    videoHash = {}
    audioHash = {}
    baseFilename = File.basename(file)
    mediaFile["filename"]=baseFilename

    fnumber = get_fnum(baseFilename)
    if (fnumber==nil)
      fnumber="N/A"
    end
    mediaFile["fumber"]=fnumber

    fileCreation = File.birthtime(file)
    mediaFile["created"]=fileCreation

    lastModified = File.mtime(file)
    mediaFile["modified"]=lastModified

    fileSizeB = File.size(file)
    fileSizeKB = (fileSizeB/1024).round(2)
    filesizeMB = (fileSizeB/1048576.0).round(2)
    fileSizeGB = (fileSizeB/1073741824.0).round(2)

    if (fileSizeB > 1073741824)
      fileSize = fileSizeGB.to_s + " GB"
    elsif (fileSizeB > 1048576)
      fileSize = filesizeMB.to_s + " MB"
    elsif (fileSizeB > 1024)
      fileSize = fileSizeKB.to_s + " KB"
    else
      fileSize = fileSizeB.to_s + " Bytes"
    end

    mediaFile["size"]=fileSize

    puts "Generating md5 checksum for #{file}..."
    md5 = Digest::MD5.file(file).hexdigest
    puts "md5 checksum for #{file} is #{md5}"

    mediaFile["checksum"] = md5

    mediaInfoFile = MediaInfo.new file
    
    #binding.pry
    general = mediaInfoFile.general[0]
    generalVars = general.instance_variables
    generalVars.each { |gvar|
      methodName = gvar.to_s.delete("@").to_sym
      value = general.send methodName
      generalHash[methodName] = value
    }
    #binding.pry
    
    if mediaInfoFile.video?
      video = mediaInfoFile.video[0]
      videoVars = video.instance_variables
      videoVars.each { |var|
        methodName = var.to_s.delete("@").to_sym
        value = video.send methodName
        videoHash[methodName] = value
      }
    end
    audioCnt = 0
    if mediaInfoFile.audio?
      audioStreams = mediaInfoFile.audio
      audioStreams.each { |audio|
        audioCnt = audioCnt + 1
        audioLabel = audioCnt.to_s
        audioChannel = {}
        audioVars = audio.instance_variables
        audioVars.each { |avar|
          methodName = avar.to_s.delete("@").to_sym
          value = audio.send methodName
          audioChannel[methodName] = value
        }
        audioHash[audioLabel.to_sym] = audioChannel
      }
    end
    mediaFile["General"]=generalHash
    mediaFile["VideoStream"]=videoHash
    mediaFile["AudioStream"]=audioHash
    saveAsset(mediaFile)
  end
end
