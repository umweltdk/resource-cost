# coding: UTF-8
require 'csv'
require 'aws-sdk'

REGIONS = %w(eu-west-1 eu-central-1 us-east-1 us-west-1 us-west-2 ap-northeast-1 ap-northeast-2
  ap-southeast-1 ap-southeast-2 sa-east-1)

def find_tag(tags, tag)
  r = tags.find {|t| t.key == tag}
  r.value if r
end

def lookup_ec2_tags(ids)
  result = {}
  REGIONS.each do |region|
    client = Aws::EC2::Client.new(region: region)
    ids.each_slice(180) do |slice|
      arn_map = {}
      slice = slice.map do |s|
        arn = parse_arn(s)
        if arn
          arn_map[arn[:resource_id]] = s
          arn[:resource_id]
        else
          arn_map[s] = s
          s
        end
      end
      client.describe_tags({dry_run: false,
                            filters: [{
                                          name: 'resource-id',
                                          values: slice
                                      },
                                      {
                                          name: 'key',
                                          values: ['Customer', 'Name']
                                      }
                            ]
                           }). each do |tags|
        tags.tags.each do |tag|
          current = result[arn_map[tag.resource_id]] || {region: region}
          current[tag.key.to_sym] = tag.value
          result[arn_map[tag.resource_id]] = current
        end
      end

    end
  end
  result
end

def lookup_ec2_volume(ids)
  lookup_ec2_tags(ids)
end

ARN_PARSE = /^arn:aws:(?<service>.+):(?<region>.*):(?<account>.*):(?<service_type>.+)\/(?<resource_id>.+)$/
def parse_arn(arn)
  if /^arn:aws:(?<service>.+):(?<region>.*):(?<account>.*):(?<service_type>.+)\/(?<resource_id>.+)$/ =~ arn
    {
      arn: arn,
      service: service,
      region: region,
      account: account,
      type: service_type,
      resource_id: resource_id
    }
  end
end

def lookup_elb(arns)
  result = {}
  arns = arns.map {|a| parse_arn(a)}.group_by { |a| a[:region] }
  arns.each do |region, lbs|
    client = Aws::ElasticLoadBalancing::Client.new(region: region)
    names = lbs.map { |lb| lb[:resource_id] }
    actual = [].to_set
    client.describe_load_balancers.each do |lb_page|
      actual.merge(lb_page.load_balancer_descriptions.map {|lb| lb.load_balancer_name})
    end
    names.delete_if {|name| !actual.include?(name)}
    unless names.empty?
      resp = client.describe_tags(load_balancer_names: names)
      resp.tag_descriptions.each do |desc|
        desc.tags.each do |tag|
          arn = "arn:aws:elasticloadbalancing:#{region}:#{lbs.first[:account]}:loadbalancer/#{desc.load_balancer_name}"
          current = result[arn] || {region: region, Name: desc.load_balancer_name}
          current[tag.key.to_sym] = tag.value
          result[arn] = current
        end
      end
    end
  end
  result
end

def lookup_route_53_hosted_zone(arns)
  result = {}
  client = Aws::Route53::Client.new(region: 'us-east-1')
  arns.each do |a|
    arn = parse_arn(a)
    begin
      zone = client.get_hosted_zone({id: arn[:resource_id]})
      result[a] = { Name: zone.hosted_zone.name }
    rescue Aws::Route53::Errors::NoSuchHostedZone => e
    end
  end
  result
end

def lookup_cloud_front(ids)
  {}
end

def lookup_s3_bucket(ids)
  result = {}
  ids.each do |bucket|
    REGIONS.each do |region|
      begin
        client = Aws::S3::Client.new(region: region)
        tags = client.get_bucket_tagging({bucket: bucket})
        tags.tag_set.each do |tag|
          current = result[bucket] || {region: region, Name: bucket}
          current[tag.key.to_sym] = tag.value
          result[bucket] = current
        end
        break
      rescue Aws::S3::Errors::NoSuchBucket => e
      rescue Aws::S3::Errors::PermanentRedirect => e
      end
    end
  end
  result
end


def lookup_ec2_instance(ids)
  lookup_ec2_tags(ids)
end

def lookup_ec2_ip(ips)
  instances = {}
  REGIONS.each do |region|
    client = Aws::EC2::Client.new(region: region)
    ips.each_slice(180) do |slice|
      client.describe_addresses(filters: [{
                                              name: 'public-ip',
                                              values: slice
                                          }]).each do |resp|
        resp.addresses.each do |address|
          instances[address.public_ip] = address.instance_id if address.instance_id
        end
      end
    end
  end
  tags = lookup_ec2_tags(instances.values)
  result = {}
  instances.each do |key, val|
    result[key] = tags[val] if tags[val]
  end
  result
end

def lookup_ec2_unknown(ips)
  {}
end

def lookup_unknown(ips)
  {}
end

def identify_resource(resource)
  if resource[:product_name] == 'Amazon Simple Storage Service'
    :s3_bucket
  elsif resource[:product_name] == 'Amazon CloudFront'
    :cloud_front
  elsif resource[:product_name] == 'Amazon Route 53'
    :route_53_hosted_zone
  elsif resource[:product_name] == 'Amazon Elastic Compute Cloud'
    if resource[:id].start_with?('vol-')
      :ec2_volume
    elsif resource[:id].start_with?('i-') || resource[:id].start_with?('arn:aws:ec2:')
      :ec2_instance
    elsif resource[:id].start_with?('arn:aws:elasticloadbalancing:')
      :elb
    elsif /^\d+\.\d+\.\d+\.\d+$/  =~ resource[:id]
      :ec2_ip
    else
      :ec2_unknown
    end
  else
    :unknown
  end
end

def process_csv(io_in, io_out, skip_customer=false)
  input  = CSV.new(io_in, headers:true)

  group = {}

  input.each do |row|
    next unless row['RecordType'] == 'LineItem'
    next if skip_customer && row['user:Customer'] != ''
    id = row['ResourceId'] || ''
    g = group[id] || {product_name: '', blended_cost: 0.0, un_blended_cost: 0.0}
    g[:id] = id
    g[:original_customer] = row['user:Customer'] unless row['user:Customer'] == ''
    g[:original_name] = row['user:Name'] unless row['user:Name'] == ''
    g[:product_name] = row['ProductName'] unless id == ''
    if row['Cost']
      g[:blended_cost] += row['Cost'].to_f
      g[:un_blended_cost] += row['Cost'].to_f
    else
      g[:blended_cost] += row['BlendedCost'].to_f
      g[:un_blended_cost] += row['UnBlendedCost'].to_f
    end
    group[id] = g
  end

  group.each do |k, v|
    next if k == ''
    t = identify_resource(v)
    v[:resource_type] = t if t
  end

  group.values.group_by {|g| g[:resource_type]}.each do |k, v|
    next unless k
    ids = v.map {|l| l[:id] }
    result = send("lookup_#{k}", ids)
    v.each do |resource|
      if result[resource[:id]]
        resource[:resource] = result[resource[:id]]
      end
    end
  end


  output = CSV.new(io_out, headers: true, write_headers: true)
  output << %w(ProductName Type Region ResourceId Name OriginalName Customer OriginalCustomer BlendedCost UnBlendedCost)
  group.each do |k, v|
    if v[:resource]
      output << [v[:product_name], v[:resource_type], v[:resource][:region], v[:id],
                 v[:resource][:Name], v[:original_name], v[:resource][:Customer], v[:original_customer],
                 v[:blended_cost], v[:un_blended_cost]]
    else
      output << [v[:product_name], v[:resource_type], nil, v[:id],
                 nil, v[:original_name], nil, v[:original_customer],
                 v[:blended_cost], v[:un_blended_cost]]
    end
  end
end

if __FILE__ == $0
  process_csv(ARGF, $stdout)
end