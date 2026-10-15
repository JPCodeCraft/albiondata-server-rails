require 'rails_helper'

RSpec.describe GameinfoService, type: :service do
  subject { described_class.new(server_id) }

  let(:server_id) { 'west' }
  let(:redis) { REDIS[server_id] }
  let(:nats) { instance_double(NatsService, send: nil, close: nil) }
  let(:base_url) { 'https://gameinfo.albiononline.com/api/gameinfo' }
  let(:pages) { {} }

  before do
    clear_gameinfo_keys
    allow(Sidekiq.logger).to receive(:info)
    allow(NatsService).to receive(:new).with(server_id).and_return(nats)
    allow(subject).to receive(:sleep)
    allow(HTTParty).to receive(:get) do |url, **_options|
      body = pages.fetch(url, [])
      body == :not_found ? response(404, '') : response(200, body.to_json)
    end
  end

  after do
    clear_gameinfo_keys
  end

  describe '#poll_events' do
    it 'publishes new events and stops at the first page without new events' do
      pages[events_url(0)] = [event(3), event(2)]
      pages[events_url(51)] = [event(1)]
      subject.poll_events

      expect(nats).to have_received(:send).with('killevents.deduped', event(3).to_json)
      expect(nats).to have_received(:send).with('killevents.deduped', event(2).to_json)
      expect(nats).to have_received(:send).with('killevents.deduped', event(1).to_json)

      pages[events_url(0)] = [event(4), event(3)]
      subject.poll_events

      expect(nats).to have_received(:send).exactly(4).times
      expect(HTTParty).to have_received(:get).with(events_url(51), anything).twice
      expect(HTTParty).to have_received(:get).with(events_url(102), anything).once
    end

    it 'stops at the max offset' do
      event_id = 0
      allow(HTTParty).to receive(:get) { response(200, [event(event_id += 1)].to_json) }

      subject.poll_events

      expect(HTTParty).to have_received(:get).exactly(20).times
      expect(HTTParty).to have_received(:get).with(events_url(969), anything)
    end

    it 'tracks the battle of each new event' do
      pages[events_url(0)] = [event(2, battle_id: 1), event(1, battle_id: 1)]
      subject.poll_events

      expect(redis.hget(GameinfoService::PENDING_BATTLE_KILLS_KEY, '1')).to eq('2')
      expect(redis.zscore(GameinfoService::PENDING_BATTLES_KEY, '1')).to be_present
    end

    it 'skips the run when a previous run holds the lock' do
      redis.set('gameinfo:lock:events', 'other')
      subject.poll_events

      expect(HTTParty).not_to have_received(:get)
    end

    it 'releases the lock after a successful run' do
      subject.poll_events

      expect(redis.get('gameinfo:lock:events')).to be_nil
    end

    it 'does not release a lock taken by another run' do
      allow(HTTParty).to receive(:get) do
        redis.set('gameinfo:lock:events', 'other')
        response(200, [].to_json)
      end
      subject.poll_events

      expect(redis.get('gameinfo:lock:events')).to eq('other')
    end

    it 'keeps the lock and forgets the event when publishing fails' do
      pages[events_url(0)] = [event(1)]
      allow(nats).to receive(:send).and_raise('nats down')

      expect { subject.poll_events }.to raise_error('nats down')
      expect(redis.get('gameinfo:lock:events')).to be_present
      expect(redis.exists?('gameinfo:killevent:1')).to be(false)
      expect(nats).to have_received(:close)
    end

    it 'adds a different cache breaker to every request' do
      pages[events_url(0)] = [event(1)]
      subject.poll_events

      cache_breakers = []
      expect(HTTParty).to have_received(:get).twice do |_url, **options|
        cache_breakers << options.dig(:query, :_)
      end
      expect(cache_breakers).to all(match(/\A\h{16}\z/))
      expect(cache_breakers.uniq.size).to eq(2)
    end

    it 'retries rate limited requests' do
      allow(HTTParty).to receive(:get).and_return(response(429, ''), response(200, [event(1)].to_json), response(200, [].to_json))
      subject.poll_events

      expect(nats).to have_received(:send).with('killevents.deduped', event(1).to_json)
      expect(subject).to have_received(:sleep).with(2)
    end
  end

  describe '#poll_battles' do
    it 'publishes each listed battle once' do
      pages[battles_url(0)] = [battle(2), battle(1)]
      pages[battles_url(51)] = [battle(1)]
      subject.poll_battles
      subject.poll_battles

      expect(nats).to have_received(:send).with('battles.deduped', battle(2).to_json).once
      expect(nats).to have_received(:send).with('battles.deduped', battle(1).to_json).once
      expect(HTTParty).to have_received(:get).with(battles_url(102), anything).twice
    end

    describe 'backfill' do
      let(:battle_url) { "#{base_url}/battles/1" }

      it 'publishes a tracked battle the list never showed' do
        track_battle(1, kills: 3, last_kill_at: 20.minutes.ago)
        pages[battle_url] = battle(1)
        subject.poll_battles

        expect(nats).to have_received(:send).with('battles.deduped', battle(1).to_json)
        expect(redis.zscore(GameinfoService::PENDING_BATTLES_KEY, '1')).to be_nil
      end

      it 'waits for recent battles' do
        track_battle(1, kills: 3, last_kill_at: 5.minutes.ago)
        subject.poll_battles

        expect(HTTParty).not_to have_received(:get).with(battle_url, anything)
      end

      it 'drops battles with fewer than 3 kills without fetching them' do
        track_battle(1, kills: 2, last_kill_at: 20.minutes.ago)
        subject.poll_battles

        expect(HTTParty).not_to have_received(:get).with(battle_url, anything)
        expect(redis.zscore(GameinfoService::PENDING_BATTLES_KEY, '1')).to be_nil
      end

      it 'keeps retrying a battle that is not found yet' do
        track_battle(1, kills: 3, last_kill_at: 20.minutes.ago)
        pages[battle_url] = :not_found
        subject.poll_battles

        expect(redis.zscore(GameinfoService::PENDING_BATTLES_KEY, '1')).to be_present
      end

      it 'gives up on a battle after an hour' do
        track_battle(1, kills: 3, last_kill_at: 2.hours.ago)
        pages[battle_url] = :not_found
        subject.poll_battles

        expect(redis.zscore(GameinfoService::PENDING_BATTLES_KEY, '1')).to be_nil
        expect(redis.hget(GameinfoService::PENDING_BATTLE_KILLS_KEY, '1')).to be_nil
      end
    end
  end

  def events_url(offset)
    "#{base_url}/events?limit=51&offset=#{offset}"
  end

  def battles_url(offset)
    "#{base_url}/battles?sort=recent&limit=51&offset=#{offset}"
  end

  def event(id, battle_id: id)
    { 'EventId' => id, 'BattleId' => battle_id, 'TimeStamp' => '2026-10-15T15:20:05.7291611Z' }
  end

  def battle(id)
    { 'id' => id, 'totalKills' => 3 }
  end

  def track_battle(battle_id, kills:, last_kill_at:)
    redis.zadd(GameinfoService::PENDING_BATTLES_KEY, last_kill_at.to_i, battle_id)
    redis.hset(GameinfoService::PENDING_BATTLE_KILLS_KEY, battle_id, kills)
  end

  def response(code, body)
    double('HTTParty::Response', success?: code == 200, code: code, body: body)
  end

  def clear_gameinfo_keys
    keys = redis.keys('gameinfo:*')
    redis.del(*keys) if keys.any?
  end
end
