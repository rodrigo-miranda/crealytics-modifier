require File.expand_path('lib/combiner',File.dirname(__FILE__))
require File.expand_path('lib/txt_handler',File.dirname(__FILE__))
require File.expand_path('lib/core_extends/string',File.dirname(__FILE__))
require File.expand_path('lib/core_extends/float',File.dirname(__FILE__))

class Modifier

  KEYWORD_UNIQUE_ID = 'Keyword Unique ID'
  LAST_VALUE_WINS = ['Account ID', 'Account Name', 'Campaign', 'Ad Group', 'Keyword', 'Keyword Type', 'Subid', 'Paused', 'Max CPC', 'Keyword Unique ID', 'ACCOUNT', 'CAMPAIGN', 'BRAND', 'BRAND+CATEGORY', 'ADGROUP', 'KEYWORD']
  LAST_REAL_VALUE_WINS = ['Last Avg CPC', 'Last Avg Pos']
  INT_VALUES = ['Clicks', 'Impressions', 'ACCOUNT - Clicks', 'CAMPAIGN - Clicks', 'BRAND - Clicks', 'BRAND+CATEGORY - Clicks', 'ADGROUP - Clicks', 'KEYWORD - Clicks']
  FLOAT_VALUES = ['Avg CPC', 'CTR', 'Est EPC', 'newBid', 'Costs', 'Avg Pos']
  NUMBER_OF_COMMISSIONS = ['number of commissions']
  COMMISSIONS_FIELDS = ['Commission Value', 'ACCOUNT - Commission Value', 'CAMPAIGN - Commission Value', 'BRAND - Commission Value', 'BRAND+CATEGORY - Commission Value', 'ADGROUP - Commission Value', 'KEYWORD - Commission Value']

  def initialize(saleamount_factor, cancellation_factor)
    @saleamount_factor = saleamount_factor
    @cancellation_factor = cancellation_factor
  end

  def modify(performance_data)
    input_enumerator = performance_data.save_sorted_by('Clicks').lazy_read

    combiner = Combiner.new do |value|
      value[KEYWORD_UNIQUE_ID]
    end.combine(input_enumerator)

    merger = Enumerator.new do |yielder|
      while true
        begin
          list_of_rows = combiner.next
          merged = combine_hashes(list_of_rows)
          yielder.yield(combine_values(merged))
        rescue StopIteration
          break
        end
      end
    end

    performance_data.write(merger)

  end

  private
  def combine(merged)
    result = []
    merged.each do |_, hash|
      result << combine_values(hash)
    end
    result
  end

  def combine_values(hash)
    LAST_VALUE_WINS.each do |key|
      hash[key] = hash[key].last
    end
    LAST_REAL_VALUE_WINS.each do |key|
      hash[key] = hash[key].select {|v| not (v.nil? or v == 0 or v == '0' or v == '')}.last
    end
    INT_VALUES.each do |key|
      hash[key] = hash[key][0].to_s
    end
    FLOAT_VALUES.each do |key|
      hash[key] = hash[key][0].from_german_to_f.to_german_s
    end
    NUMBER_OF_COMMISSIONS.each do |key|
      hash[key] = (@cancellation_factor * hash[key][0].from_german_to_f).to_german_s
    end
    COMMISSIONS_FIELDS.each do |key|
      hash[key] = (@cancellation_factor * @saleamount_factor * hash[key][0].from_german_to_f).to_german_s
    end
    hash
  end

  def combine_hashes(list_of_rows)
    keys = []
    list_of_rows.each do |row|
      next if row.nil?
      row.headers.each do |key|
        keys << key
      end
    end
    result = {}
    keys.each do |key|
      result[key] = []
      list_of_rows.each do |row|
        result[key] << (row.nil? ? nil : row[key])
      end
    end
    result
  end

end

performance_data = TxtHandler.new('project_2012-07-27_2012-10-10_performancedata')
modification_factor = 1
cancellaction_factor = 0.4
modifier = Modifier.new(modification_factor, cancellaction_factor)
modifier.modify(performance_data)

puts "DONE modifying"