# encoding: utf-8
#
require "logstash/inputs/threadable"
require "logstash/namespace"
require "logstash/timestamp"
require "logstash/errors"
require 'logstash/inputs/sqs/patch'

# Forcibly load all modules marked to be lazily loaded.
#
# It is recommended that this is called prior to launching threads. See
# https://aws.amazon.com/blogs/developer/threading-with-the-aws-sdk-for-ruby/.
Aws.eager_autoload!

# Pull events from an Amazon Web Services Simple Queue Service (SQS) queue.
#
# SQS is a simple, scalable queue system that is part of the
# Amazon Web Services suite of tools.
#
# Although SQS is similar to other queuing systems like AMQP, it
# uses a custom API and requires that you have an AWS account.
# See http://aws.amazon.com/sqs/ for more details on how SQS works,
# what the pricing schedule looks like and how to setup a queue.
#
# To use this plugin, you *must*:
#
#  * Have an AWS account
#  * Setup an SQS queue
#  * Create an identify that has access to consume messages from the queue.
#
# The "consumer" identity must have the following permissions on the queue:
#
#  * `sqs:ChangeMessageVisibility`
#  * `sqs:ChangeMessageVisibilityBatch`
#  * `sqs:DeleteMessage`
#  * `sqs:DeleteMessageBatch`
#  * `sqs:GetQueueAttributes`
#  * `sqs:GetQueueUrl`
#  * `sqs:ListQueues`
#  * `sqs:ReceiveMessage`
#
# Typically, you should setup an IAM policy, create a user and apply the IAM policy to the user.
# A sample policy is as follows:
# [source,json]
#     {
#       "Statement": [
#         {
#           "Action": [
#             "sqs:ChangeMessageVisibility",
#             "sqs:ChangeMessageVisibilityBatch",
#             "sqs:GetQueueAttributes",
#             "sqs:GetQueueUrl",
#             "sqs:ListQueues",
#             "sqs:SendMessage",
#             "sqs:SendMessageBatch"
#           ],
#           "Effect": "Allow",
#           "Resource": [
#             "arn:aws:sqs:us-east-1:123456789012:Logstash"
#           ]
#         }
#       ]
#     }
#
# See http://aws.amazon.com/iam/ for more details on setting up AWS identities.
#
class LogStash::Inputs::SQS < LogStash::Inputs::Threadable
  CredentialConfig = Struct.new(
    :access_key_id,
    :secret_access_key,
    :session_token,
    :profile,
    :instance_profile_credentials_retries,
    :instance_profile_credentials_timeout,
    :region)

  MAX_TIME_BEFORE_GIVING_UP = 60
  MAX_MESSAGES_TO_FETCH = 10 # Between 1-10 in the AWS-SDK doc
  SENT_TIMESTAMP = "SentTimestamp"
  SQS_ATTRIBUTES = [SENT_TIMESTAMP]
  BACKOFF_SLEEP_TIME = 1
  BACKOFF_FACTOR = 2
  DEFAULT_POLLING_FREQUENCY = 20

  config_name "sqs"

  default :codec, "json"

  config :additional_settings, :validate => :hash, :default => {}

  # Name of the SQS Queue name to pull messages from. Note that this is just the name of the queue, not the URL or ARN.
  config :queue, :validate => :string, :required => true

  # Account ID of the AWS account which owns the queue.
  config :queue_owner_aws_account_id, :validate => :string, :required => false

  # Name of the event field in which to store the SQS message ID
  config :id_field, :validate => :string

  # Name of the event field in which to store the SQS message MD5 checksum
  config :md5_field, :validate => :string

  # Name of the event field in which to store the SQS message Sent Timestamp
  config :sent_timestamp_field, :validate => :string

  # Polling frequency, default is 20 seconds
  config :polling_frequency, :validate => :number, :default => DEFAULT_POLLING_FREQUENCY

  config :region, :validate => :string, :default => "us-east-1"

  # This plugin uses the AWS SDK and supports several ways to get credentials, which will be tried in this order:
  #
  # 1. Static configuration, using `access_key_id` and `secret_access_key` params or `role_arn` in the logstash plugin config
  # 2. External credentials file specified by `aws_credentials_file`
  # 3. Environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
  # 4. Environment variables `AMAZON_ACCESS_KEY_ID` and `AMAZON_SECRET_ACCESS_KEY`
  # 5. IAM Instance Profile (available when running inside EC2)
  config :access_key_id, :validate => :string

  # The AWS Secret Access Key
  config :secret_access_key, :validate => :string

  # Profile
  config :profile, :validate => :string, :default => "default"

  # The AWS Session token for temporary credential
  config :session_token, :validate => :password

  # URI to proxy server if required
  config :proxy_uri, :validate => :string

  # Custom endpoint to connect to s3
  config :endpoint, :validate => :string

  # The AWS IAM Role to assume, if any.
  # This is used to generate temporary credentials typically for cross-account access.
  # See https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html for more information.
  config :role_arn, :validate => :string

  # Session name to use when assuming an IAM role
  config :role_session_name, :validate => :string, :default => "logstash"

  # Path to YAML file containing a hash of AWS credentials.
  # This file will only be loaded if `access_key_id` and
  # `secret_access_key` aren't set. The contents of the
  # file should look like this:
  #
  # [source,ruby]
  # ----------------------------------
  #     :access_key_id: "12345"
  #     :secret_access_key: "54321"
  # ----------------------------------
  #
  config :aws_credentials_file, :validate => :string

  config :force_path_style, :validate => :string, :default => "true"

  config :ssl_verify_peer, :validate => :string, :default => "false"

  config :profile, :validate => :string, :default => 'logstash'

  attr_reader :poller

  def register
    require "aws-sdk-sqs"
    @logger.info("Registering SQS input", :queue => @queue, :queue_owner_aws_account_id => @queue_owner_aws_account_id)

    setup_queue
  end

  def queue_url(aws_sqs_client)
    if @queue_owner_aws_account_id
      return aws_sqs_client.get_queue_url({:queue_name => @queue, :queue_owner_aws_account_id => @queue_owner_aws_account_id})[:queue_url]
    else
      return aws_sqs_client.get_queue_url(:queue_name => @queue)[:queue_url]
    end
  end

  def aws_options_hash
    opts = {}

    if @access_key_id.is_a?(NilClass) ^ @secret_access_key.is_a?(NilClass)
      @logger.warn("Likely config error: Only one of access_key_id or secret_access_key was provided but not both.")
    end

    credential_config = CredentialConfig.new(@access_key_id, @secret_access_key, @session_token, @profile, 0, 1, @region)
    @credentials = Aws::CredentialProviderChain.new(credential_config).resolve

    opts[:credentials] = @credentials

    opts[:http_proxy] = @proxy_uri if @proxy_uri

    if self.respond_to?(:aws_service_endpoint)
      # used by CloudWatch to basically do the same as bellow (returns { region: region })
      opts.merge!(self.aws_service_endpoint(@region))
    else
      # NOTE: setting :region works with the aws sdk (resolves correct endpoint)
      opts[:region] = @region
    end

    if !@endpoint.is_a?(NilClass)
      opts[:endpoint] = @endpoint
    end

    opts[:force_path_style] = @force_path_style

    opts[:ssl_verify_peer] = @ssl_verify_peer

    opts[:profile] = @profile

    return opts
  end

  def setup_queue
    aws_sqs_client = Aws::SQS::Client.new(aws_options_hash || {})
    poller = Aws::SQS::QueuePoller.new(queue_url(aws_sqs_client), :client => aws_sqs_client)
    poller.before_request { |stats| throw :stop_polling if stop? }

    @poller = poller
  rescue Aws::SQS::Errors::ServiceError, Seahorse::Client::NetworkingError => e
    @logger.error("Cannot establish connection to Amazon SQS", exception_details(e))
    raise LogStash::ConfigurationError, "Verify the SQS queue name and your credentials"
  end

  def polling_options
    { 
      :max_number_of_messages => MAX_MESSAGES_TO_FETCH,
      :attribute_names => SQS_ATTRIBUTES,
      :wait_time_seconds => @polling_frequency
    }
  end

  def add_sqs_data(event, message)
    event.set(@id_field, message.message_id) if @id_field
    event.set(@md5_field, message.md5_of_body) if @md5_field
    event.set(@sent_timestamp_field, convert_epoch_to_timestamp(message.attributes[SENT_TIMESTAMP])) if @sent_timestamp_field
    event
  end

  def handle_message(message, output_queue)
    @codec.decode(message.body) do |event|
      add_sqs_data(event, message)
      decorate(event)
      output_queue << event
    end
  end

  def run(output_queue)
    @logger.debug("Polling SQS queue", :polling_options => polling_options)

    run_with_backoff do
      poller.poll(polling_options) do |messages, stats|
        break if stop?
        messages.each {|message| handle_message(message, output_queue) }
        @logger.debug("SQS Stats:", :request_count => stats.request_count,
                      :received_message_count => stats.received_message_count,
                      :last_message_received_at => stats.last_message_received_at) if @logger.debug?
      end
    end
  end

  private

  # Runs an AWS request inside a Ruby block with an exponential backoff in case
  # we experience a ServiceError.
  #
  # @param [Block] block Ruby code block to execute.
  def run_with_backoff(&block)
    sleep_time = BACKOFF_SLEEP_TIME
    begin
      block.call
    rescue Aws::SQS::Errors::ServiceError, Seahorse::Client::NetworkingError => e
      @logger.warn("SQS error ... retrying with exponential backoff", exception_details(e, sleep_time))
      sleep_time = backoff_sleep(sleep_time)
      retry
    end
  end

  def backoff_sleep(sleep_time)
    sleep(sleep_time)
    sleep_time > MAX_TIME_BEFORE_GIVING_UP ? sleep_time : sleep_time * BACKOFF_FACTOR
  end

  def convert_epoch_to_timestamp(time)
    LogStash::Timestamp.at(time.to_i / 1000)
  end

  def exception_details(e, sleep_time = nil)
    details = { :queue => @queue, :exception => e.class, :message => e.message }
    details[:code] = e.code if e.is_a?(Aws::SQS::Errors::ServiceError) && e.code
    details[:cause] = e.original_error if e.respond_to?(:original_error) && e.original_error # Seahorse::Client::NetworkingError
    details[:sleep_time] = sleep_time if sleep_time
    details[:backtrace] = e.backtrace if @logger.debug?
    details
  end

end # class LogStash::Inputs::SQS
