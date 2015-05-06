require 'tweetstream'
require 'pg'
require 'rubygems'
require 'json'

class TweetCollector
    $go_run = true
    
	@keywords_set = false
	@keywords = ""
	@continue = true
	@db_host = ""
	@db_port = ""
	@db_database = ""
	@db_username = ""
	@db_password = ""
	@conn = nil
	@job_name = ""


	def initialize(jname)
        
        print "Setting job name..."
        
        setJob(jname)
        
        puts "done!"
        
		print "Loading database configuration file..."

		File.open("configuration", "r").each_line do |line|
			data = line.split(": ")
	
			if data[0] == "host" then
				@db_host = data[1].strip
			elsif data[0] == "database" then
				@db_database = data[1].strip
			elsif data[0] == "username" then
				@db_username = data[1].strip
			elsif data[0] == "password" then
				@db_password = data[1].strip
			elsif data[0] == "port" then
				@db_port = data[1].strip
			end
		end

		puts "done!"

		print "About to configure..."

        TweetStream.configure do |config|
            config.consumer_key       = 'o9xN90cxyfO9XZnYVveQ8GS6V'
            config.consumer_secret    = 'Raw6GltwcfUu7Bx8HhZIts7wdsDo2LN50ajYvVTOIePLSvk6WF'
            config.oauth_token        = '2424725288-mY4jXlfNS7N9JDJp2KR6m8W2BxY7aHrWn6hQveI'
            config.oauth_token_secret = 'rcEpL43xuUOGAOTIItAv2bg4PpbtZNhvSCaccfFhTLeMt'
            config.auth_method        = :oauth
		end

		puts "done!"
        
        
        puts "Job created!"

        dbConnect()
        setTrackingKeywords()
        startTracking()
        #loopitup
	end
    
    def loopitup
        while true do
            
        end
    end
    
    def stop()
        $go_run  = false
    end

	def setJob(job)
		@job_name = job
	end

	def setTrackingKeywords()
		if @job_name.nil? || @job_name == "" then
			puts "No Job Name set!"
		else
			print "Retrieving keyword tracking list..."

			@keywords = ""

			res = @conn.exec("SELECT key FROM keywords WHERE job = '#{@job_name}';")

            @keywords = res[0]["key"]

            puts @keywords

			puts "done!"

			@keywords_set = true
		end
	end

	def dbConnect()
		print 'About to connect to the database...'

		@conn = PGconn.connect(:host=>@db_host, :port=>@db_port, :dbname=>@db_database, :user=>@db_username, :password=>@db_password)

		puts 'done!'
	end 

	def stopTracking()
		@continue = false
	end

	def startTracking()
		if @keywords_set then
			print "About to connect to Twitter Stream..."

			@continue = true

			@tclient = TweetStream::Client.new

			@tclient.on_error do |message|
				puts message
			end 

			flag = false

			@tclient.track(@keywords) do |status|
				if !flag then
					print "done!\nStarting Tweet colletion...\n"
					print "Keywords used: " + @keywords + "\n"
					flag = true
				end

				puts "#{status.text}"

				tweetstring = @conn.escape_string(status.text)
				user = status.user.screen_name
				created_at = status.attrs[:created_at]
				tweet_created_at = Time.now

				if status.attrs[:coordinates].nil? then
					lat = 'NULL'
					lon = 'NULL'
				else	
					lat = status.attrs[:coordinates][:coordinates].last
					lon = status.attrs[:coordinates][:coordinates].first
				end

				res = @conn.exec("INSERT INTO tweets (tweet_user, tweet_text, tweet_time, tweet_lat, tweet_lon, tweet_job, created_at, updated_at) VALUES ('#{user}', '#{tweetstring}', '#{created_at}', #{lat}, #{lon}, '#{@job_name}', '#{created_at}', '#{tweet_created_at}')")
		
				if !@continue then
                    next
				end
			end
		else
			puts "Keywords not set. Unable to start tracking."
		end
	end
end
