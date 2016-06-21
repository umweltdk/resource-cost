# coding: UTF-8
require 'sinatra'
require 'sinatra/streaming'
require 'zip/filesystem'
require 'aws-sdk'
require 'securerandom'

require './group'

DEFAULT_BUCKET = ENV['DEFAULT_BUCKET']
DEFAULT_ACCOUNT = ENV['DEFAULT_ACCOUNT']
DEFAULT_MONTH = Date.today.strftime('%Y-%m')

get '/' do
  '<html><body><form action="download.csv" method="get">' +
      'Bucket: <input name="bucket" value="'+DEFAULT_BUCKET+'"/><br/>' +
      'Account: <input name="account" value="'+DEFAULT_ACCOUNT+'"/><br/>' +
      'Month: <input type="month" name="month" value="'+DEFAULT_MONTH+'"/><br/>' +
      'Skip resources with Customer tag: <input type="checkbox" name="skip_customer" value="true"/><br/>' +
      ' <button>Download</button>' +
      '</form></body></html>'
end

get '/download.csv' do
  bucket = params[:bucket] || DEFAULT_BUCKET
  account = params[:account] || DEFAULT_ACCOUNT
  file = "#{account}-aws-billing-detailed-line-items-with-resources-and-tags-#{params[:month]}.csv"
  temp = "tmp/#{SecureRandom.uuid}-#{params[:month]}"
  client = Aws::S3::Client.new(region: 'us-east-1')
  client.get_object({
                        response_target: "#{temp}.zip",
                        bucket: bucket,
                        key: "#{file}.zip"
                    })
  Zip::File.open("#{temp}.zip") do |zipfile|
    zipfile.extract(file, "#{temp}.csv")
  end
  File.delete("#{temp}.zip")
  content_type :csv
  headers 'Content-disposition' => "attachment; filename=grouped-#{params[:month]}.csv"
  stream do |out|
    process_csv(File.new("#{temp}.csv", 'r:UTF-8'), out, params[:skip_customer] == 'true')
    out.flush
    File.delete("#{temp}.csv")
  end
end