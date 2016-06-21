require 'csv'

def filter(*args)
  # parse options for input, output, or both
  in_options, out_options = Hash.new, {row_sep: $INPUT_RECORD_SEPARATOR}
  if args.last.is_a? Hash
    args.pop.each do |key, value|
      case key.to_s
        when /\Ain(?:put)?_(.+)\Z/
          in_options[$1.to_sym] = value
        when /\Aout(?:put)?_(.+)\Z/
          out_options[$1.to_sym] = value
        else
          in_options[key]  = value
          out_options[key] = value
      end
    end
  end
  # build input and output wrappers
  input  = CSV.new(args.shift || ARGF,    in_options)
  output = CSV.new(args.shift || $stdout, out_options)

  # read, yield, write
  input.each do |row|
    ret = yield row
    output << ret if ret
  end
end

class EqFilter
  def initialize(a, b)
    @a, @b = a, b
  end

  def call(row)
    ret = row[@a] == @b || row[@a].to_s == @b
    if $DEBUG
      $stderr.puts "#{self}: #{ret}"
    end
    ret
  end

  def inspect
    "row[#{@a}]==#{@b}"
  end

  def to_s
    inspect
  end
end

#$DEBUG=true

filters = []
while ARGV[0] == '--filter'
  ARGV.shift
  pred = ARGV.shift
  raise "invalid arguments" unless pred
  if pred.split('=', 2).size == 2
    a = pred.split('=', 2)
    pred = EqFilter.new(a[0], a[1])
  else
    raise "unknown operation #{pred}"
  end
  $stderr.puts "filtering on #{pred}"
  filters << pred
end

filter(headers:true, write_headers: true, return_headers: true) do |row|
  if row.header_row? || filters.all? {|pred| pred.call(row) }
    row
  end
end
