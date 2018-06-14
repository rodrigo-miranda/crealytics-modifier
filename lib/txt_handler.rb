require 'date'
require 'csv'

class TxtHandler

  READ_CSV_OPTIONS = { :col_sep => "\t", :headers => :first_row }
  WRITE_CSV_OPTIONS = READ_CSV_OPTIONS.merge(row_sep: "\r\n")
  LINES_PER_FILE = 120000

  def initialize(file_name)
    @input_file = most_recent(file_name)
    @sorted_file = "#{@input_file}.sorted"
  end

  def save_sorted_by(field)
    content_as_table = parse(@input_file)
    headers = content_as_table.headers
    index_of_key = headers.index(field)
    content = content_as_table.sort_by { |a| -a[index_of_key].to_i }
    write_sorted(content, headers, @sorted_file)
    self
  end

  def write(merger)
    done = false
    file_index = 0
    file_name = @input_file.gsub('.txt.sorted', '')
    while not done do
      CSV.open(file_name + "_#{file_index}.txt", "wb", WRITE_CSV_OPTIONS) do |csv|
        headers_written = false
        line_count = 0
        while line_count < LINES_PER_FILE
          begin
            merged = merger.next
            if not headers_written
              csv << merged.keys
              headers_written = true
              line_count +=1
            end
            csv << merged
            line_count +=1
          rescue StopIteration
            done = true
            break
          end
        end
        file_index += 1
      end
    end
  end
  
  def lazy_read
    Enumerator.new do |yielder|
      CSV.foreach(@sorted_file, READ_CSV_OPTIONS) do |row|
        yielder.yield(row)
      end
    end
  end

  private
  def most_recent(file_name)
    files = Dir["#{ ENV["HOME"] }/workspace/*#{file_name}*.txt"]
    files.sort_by! do |file|
      last_date = /\d+-\d+-\d+_[[:alpha:]]+\.txt$/.match file
      last_date = last_date.to_s.match /\d+-\d+-\d+/
      date = DateTime.parse(last_date.to_s)
    end
    throw RuntimeError if files.empty?
    files.last
  end

  def parse(file)
    CSV.read(file, READ_CSV_OPTIONS)
  end

  def write_sorted(content, headers, output)
    CSV.open(output, "wb", WRITE_CSV_OPTIONS) do |csv|
      csv << headers
      content.each do |row|
        csv << row
      end
    end
  end

end