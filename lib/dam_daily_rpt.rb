require 'find'
require 'pry'
require 'oci8'
require 'socket'
require 'date'

def daily_archive_report()
  today = Date.today
  output_file = "DAILY_ARCHIVE_" + today.prev_day.to_s + ".CSV"
  o = OCI8.new('diva_ro/diva_ro@lib5')
  File.open(output_file, 'w') do |exportfile|
    o.exec("SELECT RE_COMPLETION_DATE, RE_OBJECT_NAME_PARAMETER
    FROM DIVA.DP_REQUESTS INNER JOIN DP_TAPE_INSTN_CMPT_ELEMS on RE_OBJECT_NAME_PARAMETER = TE_OBJECT_NAME
    WHERE RE_COMPLETION_DATE > TRUNC(sysdate - 7) AND RE_COMPLETION_DATE < TRUNC(sysdate)
    AND RE_TYPE = 'A'
    AND RE_STATUS = 'C'
    GROUP BY RE_COMPLETION_DATE, RE_OBJECT_NAME_PARAMETER
    ORDER BY RE_COMPLETION_DATE ASC NULLS LAST") do |r|
    #puts "#{r} exact filename is found in DAM..."
      filename = r[1]
      r[2] = filename[/(F\d{9})/]
      #print r
      exportfile.puts r.join("\t")
    end
  end
  o.logoff
end

daily_archive_report
