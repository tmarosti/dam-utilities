#### Digital Asset Management (DAM) Utilities ####
#
# Launch this Ruby file from the command line
# to get started.
#
# ruby init.rb
#

APP_ROOT = File.dirname(__FILE__)

# Array of all the folders Ruby will look in
$:.unshift(File.join(APP_ROOT, 'lib'))


require 'dam_check'
require 'watch_folder'
require 'pry'
require 'dw_po_ingest'
require 'dw_iTunesPackage'


class Config
  # configure number of menu options
  @@options = ['1','2','3','4','quit']
  def self.options; @@options; end
end


def get_utility
  option = nil
  # Did the user select a valid option?
  until Config.options.include?(option)
    puts "I don't understand " + option if option
    puts "1 - DAM Check: Scripts and functions to bridge the gap between files on the SAN and in the DAM"
    puts "2 - DAM Directory Watcher: Filters files in directory before they are archived to the DAM"
    puts "3 - PO Directory Watcher (Ingest PO Details to the database)"
    puts "4 - iTunes md5 Generator (Create md5 hash for iTunes)"
    print "> "
    option = gets.chomp
  end
  return option
end

puts "\n\n<<<< Welcome to the DAM Utilities application >>>>\n\n"
puts "Please select a utility to continue.\n\n"

result = nil
until result ==:quit
  # what do you want to do? (1, 2, 3, quit)
  #	do that action
  utility = get_utility
  case utility
  when '1'
    # command line utilities used before option 2 was built, deprecated and not useful, see lib/dam_check.rb
    dam_check = DAMCheck.new('DAMFileList.txt')
    dam_check.launch!
  when '2'
    # automated script that validates files in TO_BE_DELETED directories on QX1200 and QD6000, see lib/watch_folder.rb
    dam_watch = DAMDirectoryWatcher.new(false)
    dam_watch.launch!
  when '3'
    # automated script for scraping file specifications from PO PDF files in QX1200/PURCHASE_ORDERS, see lib/dw_po_ingest.rb
    po_ingest = DW_POIngest.new()
    po_ingest.launch!
  when '4'
    itunes_md5 = DW_iTunesPackage.new()
    itunes_md5.launch!
  when 'quit'
    # type quit to exit program gracefully
    result=:quit
  else
    puts "\nNot a valid command.  Please pick a valid number from the list.\n"
  end
end


puts "\n<<<< Goodbye! >>>>\n\n\n"

#dam_check = DAMCheck.new('DAMFileList.txt')
#dam_check.launch!
