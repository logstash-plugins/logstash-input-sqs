# encoding: utf-8
require "logstash/inputs/sqs"
require "logstash/errors"
require "logstash/event"
require "logstash/json"
require "aws-sdk"
require "spec_helper"

describe LogStash::Inputs::SQS do
  let(:queue_name) { "the-infinite-pandora-box" }
  let(:queue_url) { "https://sqs.test.local/#{queue}" }
  let(:options) do
    {
      "region" => "us-east-1",
      "access_key_id" => "123",
      "secret_access_key" => "secret",
      "queue" => queue_name
    }
  end

  let(:input) { LogStash::Inputs::SQS.new(options) }

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
        let(:options) do
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
          expect(subject[id_field]).to eq(message_id)
        end

        it "add the `md5_of_body`" do
          expect(subject[md5_field]).to eq(md5_of_body)
        end

        it "add the `sent_timestamp`" do
          expect(subject[sent_timestamp_field].to_i).to eq(sent_timestamp.to_i)
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
      subject { LogStash::Inputs::SQS::new(options.merge({ "codec" => "json" })) }

      it "uses the specified codec" do
        expect(subject.decode_event(encoded_message)["bonjour"]).to eq(decoded_message["bonjour"])
      end
    end

    context "receiving messages" do
      before do 
        expect(subject).to receive(:poller).and_return(mock_sqs).at_least(:once)
      end

      it "creates logstash event" do
        expect(mock_sqs).to receive(:poll).with(anything()).and_yield([encoded_message], double("stats"))
        subject.run(queue)
        expect(queue.pop["bonjour"]).to eq(decoded_message["bonjour"])
      end
    end

    context "on errors" do
      let(:payload) { "Hello world" }

      before do
        expect(subject).to receive(:poller).and_return(mock_sqs).at_least(:once)
      end

      context "SQS errors" do
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

      context "other errors" do
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
