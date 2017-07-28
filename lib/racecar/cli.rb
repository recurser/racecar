require "optparse"
require "logger"
require "fileutils"
require "racecar/rails_config_file_loader"
require "racecar/daemon"

module Racecar
  module Cli
    def self.main(args)
      daemonize = false

      parser = OptionParser.new do |opts|
        opts.banner = "Usage: racecar MyConsumer [options]"

        opts.on("-d", "--daemonize", "Run the consumer daemonized in the background") do
          daemonize = true
        end

        opts.on("--pidfile PATH", "Save the Racecar PID to the specified file") do |path|
          Racecar.config.pidfile = File.expand_path(path)
        end

        opts.on("-r", "--require LIBRARY", "Require the LIBRARY before starting the consumer") do |lib|
          require lib
        end

        opts.on("-l", "--log LOGFILE", "Log to the specified file") do |logfile|
          Racecar.config.logfile = logfile
        end

        opts.on_tail("--version", "Show Racecar version") do
          require "racecar/version"
          $stderr.puts "Racecar #{Racecar::VERSION}"
          exit
        end
      end

      parser.parse!(args)

      consumer_name = args.first or raise Racecar::Error, "no consumer specified"

      $stderr.puts "=> Starting Racecar consumer #{consumer_name}..."

      RailsConfigFileLoader.load!

      # Find the consumer class by name.
      consumer_class = Kernel.const_get(consumer_name)

      # Load config defined by the consumer class itself.
      Racecar.config.load_consumer_class(consumer_class)

      Racecar.config.validate!

      if Racecar.config.logfile
        $stderr.puts "=> Logging to #{Racecar.config.logfile}"
        Racecar.logger = Logger.new(Racecar.config.logfile)
      end

      $stderr.puts "=> Wrooooom!"

      if daemonize
        daemon = Daemon.new(File.expand_path(Racecar.config.pidfile))

        daemon.check_pid

        $stderr.puts "=> Starting background process"
        $stderr.puts "=> Writing PID to #{daemon.pidfile}"

        if Racecar.config.logfile.nil?
          daemon.suppress_output
        else
          daemon.redirect_output(Racecar.config.logfile)
        end

        daemon.daemonize!
        daemon.write_pid
      else
        $stderr.puts "=> Ctrl-C to shutdown consumer"
      end

      processor = consumer_class.new

      Racecar.run(processor)

      $stderr.puts "=> Shut down"
    end
  end
end
