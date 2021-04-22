# encoding: utf-8
require "spec_helper"
require "logstash/devutils/rspec/shared_examples"
require "logstash/inputs/sqs"
require "logstash/errors"
require "logstash/event"
require "logstash/json"
require "aws-sdk"
require "ostruct"

describe LogStash::Inputs::SQS do
  let(:queue_name) { "the-infinite-pandora-box" }
  let(:queue_url) { "https://sqs.test.local/#{queue_name}" }
  let(:config) do
    {
      "region" => "us-east-1",
      "access_key_id" => "123",
      "secret_access_key" => "secret",
      "queue" => queue_name
    }
  end

  let(:input) { LogStash::Inputs::SQS.new(config) }
  let(:decoded_message) { { "bonjour" => "awesome" }  }
  let(:encoded_message)  { double("sqs_message", :body => LogStash::Json::dump(decoded_message)) }

  subject { input }

  let(:mock_sqs) { Aws::SQS::Client.new({ :stub_responses => true }) }


  context "with invalid credentials" do
    before do
      expect(Aws::SQS::Client).to receive(:new).and_return(mock_sqs)
      expect(mock_sqs).to receive(:get_queue_url).with({ :queue_name => queue_name }) { raise Aws::SQS::Errors::ServiceError.new("bad-something", "bad token") }
    end

    it "raises a Configuration error if the credentials are bad" do
      expect { subject.register }.to raise_error(LogStash::ConfigurationError)
    end
  end

  context "valid credentials" do
    let(:queue) { [] }

    it "doesn't raise an error with valid credentials" do
      expect(Aws::SQS::Client).to receive(:new).and_return(mock_sqs)
      expect(mock_sqs).to receive(:get_queue_url).with({ :queue_name => queue_name }).and_return({:queue_url => queue_url })
      expect { subject.register }.not_to raise_error
    end

    context "when queue_aws_account_id option is specified" do
      let(:queue_aws_account_id) { "123456789012" }
      let(:config) do
        {
          "region" => "us-east-1",
          "access_key_id" => "123",
          "secret_access_key" => "secret",
          "queue" => queue_name,
          "queue_aws_account_id" => queue_aws_account_id
        }
      end
      it "passes the option to sqs client" do
        expect(Aws::SQS::Client).to receive(:new).and_return(mock_sqs)
        expect(mock_sqs).to receive(:get_queue_url).with({ :queue_name => queue_name, :queue_owner_aws_account_id => queue_aws_account_id }).and_return({:queue_url => queue_url })
        expect { subject.register }.not_to raise_error
      end
    end

    context "when interrupting the plugin" do
      before do
        expect(Aws::SQS::Client).to receive(:new).and_return(mock_sqs)
        expect(mock_sqs).to receive(:get_queue_url).with({ :queue_name => queue_name }).and_return({:queue_url => queue_url })
        expect(subject).to receive(:poller).and_return(mock_sqs).at_least(:once)

        # We have to make sure we create a bunch of events
        # so we actually really try to stop the plugin.
        # 
        # rspec's `and_yield` allow you to define a fix amount of possible 
        # yielded values and doesn't allow you to create infinite loop.
        # And since we are actually creating thread we need to make sure
        # we have enough work to keep the thread working until we kill it..
        #
        # I haven't found a way to make it rspec friendly
        mock_sqs.instance_eval do
          def poll(polling_options = {})
            loop do
              yield [OpenStruct.new(:body => LogStash::Json::dump({ "message" => "hello world"}))], OpenStruct.new
            end
          end
        end
      end

      it_behaves_like "an interruptible input plugin"
    end

    context "enrich event" do
      let(:event) { LogStash::Event.new }

      let(:message_id) { "123" }
      let(:md5_of_body) { "dr strange" }
      let(:sent_timestamp) { LogStash::Timestamp.new }
      let(:epoch_timestamp) { (sent_timestamp.utc.to_f * 1000).to_i }

      let(:id_field) { "my_id_field" }
      let(:md5_field) { "my_md5_field" }
      let(:sent_timestamp_field) { "my_sent_timestamp_field" }

      let(:message) do
        double("message", :message_id => message_id, :md5_of_body => md5_of_body, :attributes => { LogStash::Inputs::SQS::SENT_TIMESTAMP  => epoch_timestamp } )
      end 

      subject { input.add_sqs_data(event, message) }

      context "when the option is specified" do
        let(:config) do
          {
            "region" => "us-east-1",
            "access_key_id" => "123",
            "secret_access_key" => "secret",
            "queue" => queue_name,
            "id_field" => id_field,
            "md5_field" => md5_field,
            "sent_timestamp_field" => sent_timestamp_field
          }
        end

        it "add the `message_id`" do
          expect(subject.get(id_field)).to eq(message_id)
        end

        it "add the `md5_of_body`" do
          expect(subject.get(md5_field)).to eq(md5_of_body)
        end

        it "add the `sent_timestamp`" do
          expect(subject.get(sent_timestamp_field).to_i).to eq(sent_timestamp.to_i)
        end
      end

      context "when the option isn't specified" do
        it "doesnt add the `message_id`" do
          expect(subject).not_to include(id_field)
        end

        it "doesnt add the `md5_of_body`" do
          expect(subject).not_to include(md5_field)
        end

        it "doesnt add the `sent_timestamp`" do
          expect(subject).not_to include(sent_timestamp_field)
        end
      end
    end

    context "when decoding body" do
      subject { LogStash::Inputs::SQS::new(config.merge({ "codec" => "json" })) }

      it "uses the specified codec" do
        subject.handle_message(encoded_message, queue)
        expect(queue.pop.get("bonjour")).to eq(decoded_message["bonjour"])
      end
    end

    context "receiving messages" do
      before do 
        expect(subject).to receive(:poller).and_return(mock_sqs).at_least(:once)
      end

      it "creates logstash event" do
        expect(mock_sqs).to receive(:poll).with(anything()).and_yield([encoded_message], double("stats"))
        subject.run(queue)
        expect(queue.pop.get("bonjour")).to eq(decoded_message["bonjour"])
      end

      context 'can create multiple events' do
        require "logstash/codecs/json_lines"
        let(:config) { super().merge({ "codec" => "json_lines" }) }
        let(:first_message) { { "sequence" => "first" } }
        let(:second_message) { { "sequence" => "second" } }
        let(:encoded_message)  { double("sqs_message", :body => "#{LogStash::Json::dump(first_message)}\n#{LogStash::Json::dump(second_message)}\n") }

        it 'creates multiple events' do
          expect(mock_sqs).to receive(:poll).with(anything()).and_yield([encoded_message], double("stats"))
          subject.run(queue)
          events = queue.map{ |e|e.get('sequence')}
          expect(events).to match_array([first_message['sequence'], second_message['sequence']])
        end
      end
    end

    context "on errors" do
      let(:payload) { "Hello world" }

      before do
        expect(subject).to receive(:poller).and_return(mock_sqs).at_least(:once)
      end

      context "SQS error" do
        it "retry to fetch messages" do
          # change the poller implementation to raise SQS errors.
          had_error = false
          
          # actually using the child of `Object` to do an expectation of `#sleep`
          expect(subject).to receive(:sleep).with(LogStash::Inputs::SQS::BACKOFF_SLEEP_TIME)
          expect(mock_sqs).to receive(:poll).with(anything()).at_most(2) do
            unless had_error
              had_error = true
              raise Aws::SQS::Errors::ServiceError.new("testing", "testing exception")
            end

            queue << payload
          end

          subject.run(queue)

          expect(queue.size).to eq(1)
          expect(queue.pop).to eq(payload)
        end
      end

      context "SQS error (retries)" do

        it "retry to fetch messages" do
          sleep_time = LogStash::Inputs::SQS::BACKOFF_SLEEP_TIME
          expect(subject).to receive(:sleep).with(sleep_time)
          expect(subject).to receive(:sleep).with(sleep_time * 2)
          expect(subject).to receive(:sleep).with(sleep_time * 4)

          error_count = 0
          expect(mock_sqs).to receive(:poll).with(anything()).at_most(4) do
            error_count += 1
            if error_count <= 3
              raise Aws::SQS::Errors::QueueDoesNotExist.new("testing", "testing exception (#{error_count})")
            end

            queue << payload
          end

          subject.run(queue)

          expect(queue.size).to eq(1)
          expect(queue.pop).to eq(payload)
        end

      end

      context "networking error" do

        before(:all) { require 'seahorse/client/networking_error' }

        it "retry to fetch messages" do
          sleep_time = LogStash::Inputs::SQS::BACKOFF_SLEEP_TIME
          expect(subject).to receive(:sleep).with(sleep_time).twice

          error_count = 0
          expect(mock_sqs).to receive(:poll).with(anything()).at_most(5) do
            error_count += 1
            if error_count == 1
              raise Seahorse::Client::NetworkingError.new(Net::OpenTimeout.new, 'timeout')
            end
            if error_count == 3
              raise Seahorse::Client::NetworkingError.new(SocketError.new('spec-error'))
            end

            queue << payload
          end

          subject.run(queue)
          subject.run(queue)

          expect(queue.size).to eq(2)
          expect(queue.pop).to eq(payload)
        end

      end

      context "other error" do
        it "stops executing the code and raise the exception" do
          expect(mock_sqs).to receive(:poll).with(anything()).at_most(2) do
            raise RuntimeError
          end

          expect { subject.run(queue) }.to raise_error(RuntimeError)
        end
      end
    end
  end
end
