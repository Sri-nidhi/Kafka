auth = tweepy.OAuthHandler("63yIXmZMyGWr5YcZiL2x1zh0F", "Dgl4l0DO107LkLDbeKRmhYfgZoBNhKrfrjXyHCvV7eIvNJOPxL")
auth.set_access_token("2500614758-F5aYsuPs2oYtNrrZ4uiADmjRZq1SXhvZaA6MtDE", "XOruQ7foYREOPZSdiRtbae5y96aRdGKvkPEFEsx92cAWf")

api = tweepy.API(auth)
class KafkaPushListener(StreamListener):
    def __init__(self):
        #localhost:9092
        self.client = pykafka.KafkaClient("0.0.0.0:9092")

    #Get Producer that has topic name is Twitter
        self.producer = self.client.topics[bytes("twitter", "ascii")].get_producer()

    def on_data(self, data):

        try:
            json_data = json.loads(data)

            send_data = '{}'
            json_send_data = json.loads(send_data)
            json_send_data['id'] = json_data['id']
            json_send_data['text'] = json_data['text']
            json_send_data['hashtags'] = re.findall(r"#(\w+)", json_send_data['text'])
            json_send_data['senti_val'] =  afinn.score(json_data['text'])
            json_send_data['timestamp'] = json_data['created_at']
#
#             print(json_send_data['text'], " >>>>>>>>>>>>>>>>>>>> ", json_send_data['hashtags'])

            self.producer.produce(bytes(json.dumps(json_send_data),'ascii'))
            return True
        except KeyError:
            return True



    def on_error(self, status):
        print(status)
        return True
afinn = Afinn()
twitter_stream = Stream(auth, KafkaPushListener())
twitter_stream.filter(track=['#fashion'])

