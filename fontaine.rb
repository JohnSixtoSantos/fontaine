require 'sinatra'
require 'json'
require 'pg'
require 'daemons'
require_relative 'TweetCollector'

set :server, "thin"
set :lock, true

$keywords_set = false
$keywords = ""
$continue = true
$db_host = ""
$db_port = ""
$db_database = ""
$db_username = ""
$db_password = ""
$conn = nil

def setupDB()
    print "Loading database configuration file..."
    
    File.open("configuration", "r").each_line do |line|
        data = line.split(": ")
        
        if data[0] == "host" then
            $db_host = data[1].strip
        elsif data[0] == "database" then
            $db_database = data[1].strip
        elsif data[0] == "username" then
            $db_username = data[1].strip
        elsif data[0] == "password" then
            $db_password = data[1].strip
        elsif data[0] == "port" then
            $db_port = data[1].strip
        end
    end
    
    puts "done!"
end

def dbConnect()
    print 'About to connect to the database...'
    
    begin
        $conn = PGconn.connect(:host => $db_host, :port => $db_port, :dbname => $db_database, :user => $db_username, :password => $db_password)
    rescue
        puts "Could not establish a database connection! Check config."
    end
    puts 'done!'
end

setupDB()
dbConnect()

$jobs = {}
$jobs_threads = {}

get '/delete/:job_name' do
    jname = params['job_name']
    
    res = $conn.exec("DELETE FROM jobs WHERE job_name = '#{jname}';")
    
    content_type :json
    {:status => 0, :note => "Successfully deleted Job: #{jname}.}"}.to_json
end

get '/jobs' do
    res = $conn.exec("SELECT job_name, description FROM jobs;")
    
    jlist = res.map {|row| row["job_name"]}
    dlist = res.map {|row| row["description"]}
    
    content_type :json
    {:jobs => jlist, :count => jlist.size, :descriptions => dlist.to_json, :note => "These are the jobs ready to be run."}.to_json
end

post '/new/:job_name' do
    jname = params['job_name']
    tkeys = params['tweet_keys']
    desc = params['description']
    
    if tkeys.nil? || tkeys == '' then
        content_type :json
        { :status => '2', :note => 'No tweet_keys provided!' }.to_json
    elsif desc.nil? || desc == ''
        content_type :json
        { :status => '3', :note => 'No description provided!' }.to_json
    else
        begin
            res = $conn.exec("INSERT INTO jobs (job_name, description) VALUES ('#{jname}', '#{desc}');")
            res = $conn.exec("INSERT INTO keywords (key, job) VALUES ('#{tkeys}', '#{jname}');")
        
            content_type :json
            { :status => 0, :note => 'Job creation successful!' }.to_json
        rescue Exception => e
            content_type :json
            { :status => 1, :note => 'Job creation failed. Job name already exists.' }.to_json
        end
    end
end

get '/start/:job_name' do
    jname = params['job_name']
    
    print "Checking if job #{jname} exists..."
    
    res = $conn.exec("SELECT * FROM jobs WHERE job_name = '#{jname}';")
    
    if res[0]["job_name"] == jname then
        puts "done!"
    end

    print "Checking if job #{jname} is running..."

    if $jobs["#{jname}"].nil? then
       puts "no."
       
       ppid = Process.pid
       
       $jobs[jname] = child_pid = fork
       
       if ppid == Process.pid then
           #parent
           
           puts "Job #{jname} started!"
           
           $jobs[jname] = child_pid
           
           content_type :json
           { :status => 0, :job_name => jname, :notes => "Job #{jname} started!"}.to_json
       else
            #child
       
            puts "Child process running!"
       
            TweetCollector.new(jname)
       end
    else
       puts "yes."
       puts "Job starting failed."
       
       content_type :json
       { :status => 1, :job_name => jname, :notes => "Job #{jname} is running, starting failed."}.to_json
    end
end

get '/kill/:job_name' do
    jname = params['job_name']
    
    if $jobs["#{jname}"].nil? then
        content_type :json
        { :status => 1, :job_name => jname, :notes => "Job #{jname} not running."}.to_json
    else
    
        pid = $jobs[jname]
        
        system("kill -KILL #{pid}")
    
        $jobs[jname] = nil
    
        content_type :json
        { :status => 0, :job_name => jname, :notes => "Job #{jname} has been stopped."}.to_json
    end
end

get '/all' do
    content_type :json
    { :count => $jobs.size, :job_names => $jobs.keys.to_json }.to_json
end

get '/status/:job_name' do
    jname = params['job_name']
    
    if $jobs["#{jname}"].nil? then
        content_type :json
        { :status => 1, :job_name => jname, :notes => "Job #{jname} not running."}.to_json
    else
        content_type :json
        { :status => 0, :job_name => jname, :notes => "Job #{jname} is running."}.to_json
    end
end
