describe MarketHistoryDedupeService, type: :service do

  describe '.ticks_to_time' do
    it 'does not process if the sha256 hash is found in redis' do
      data = { 'foo' => 'bar' }
      json_data = data.to_json
      sha256 = Digest::SHA256.hexdigest(json_data)
      allow(REDIS).to receive(:get).with("HISTORY_RECORD_SHA256:#{sha256}").and_return('1')
      expect(NatsService).not_to receive(:send)
      expect(MarketHistoryProcessorWorker).not_to receive(:perform_async)
      MarketHistoryDedupeService.dedupe(data)
    end

    it 'returns nil if AlbionId is 0' do
      data = { 'AlbionId' => 0 }
      expect(MarketHistoryDedupeService.dedupe(data)).to eq(nil)
    end

    it 'returns a StandardError if the AlbionID is not found in redis' do
      data = { 'foo' => 'bar', 'AlbionId' => 1234 }
      allow(REDIS).to receive(:get).and_return(nil)
      allow(REDIS).to receive(:hget).with('ITEM_IDS', 1234).and_return(nil)
      expect { MarketHistoryDedupeService.dedupe(data) }.to raise_error(StandardError)
    end

    it 'sends data to NatsService and MarketHistoryProcessorWorker' do
      data = { 'foo' => 'bar', 'AlbionId' => 1234 }
      expected_data = { 'foo' => 'bar', 'AlbionId' => 1234, 'AlbionIdString' => 'SOME_ITEM_ID' }
      allow(REDIS).to receive(:get).and_return(nil)
      allow(REDIS).to receive(:hget).with('ITEM_IDS', 1234).and_return('SOME_ITEM_ID')
      expect(NatsService).to receive(:send).with('markethistories.deduped', expected_data.to_json)
      expect(MarketHistoryProcessorWorker).to receive(:perform_async).with(expected_data.to_json)
      MarketHistoryDedupeService.dedupe(data)
    end
  end
end