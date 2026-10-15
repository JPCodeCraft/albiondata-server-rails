class GameinfoService
  BASE_URLS = {
    'west' => 'https://gameinfo.albiononline.com',
    'east' => 'https://gameinfo-sgp.albiononline.com',
    'europe' => 'https://gameinfo-ams.albiononline.com'
  }.freeze

  KILL_EVENTS_TOPIC = 'killevents.deduped'
  BATTLES_TOPIC = 'battles.deduped'

  PAGE_SIZE = 51
  # The events feed rejects offsets above 1000.
  EVENTS_MAX_OFFSET = 1000
  EVENTS_PARALLEL_PAGES = ENV.fetch('GAMEINFO_EVENTS_PARALLEL_PAGES', 4).to_i
  BATTLES_MAX_OFFSET = ENV.fetch('GAMEINFO_BATTLES_MAX_OFFSET', 500).to_i

  LOCK_TTL_SECONDS = ENV.fetch('GAMEINFO_LOCK_TTL_SECONDS', 180).to_i
  MAX_RUNTIME_SECONDS = ENV.fetch('GAMEINFO_MAX_RUNTIME_SECONDS', 90).to_i
  # The API is often slow. A run stops starting requests after MAX_RUNTIME_SECONDS, so its worst case
  # is MAX_RUNTIME_SECONDS + HTTP_OPEN_TIMEOUT_SECONDS + HTTP_READ_TIMEOUT_SECONDS, within LOCK_TTL_SECONDS.
  HTTP_OPEN_TIMEOUT_SECONDS = 10
  HTTP_READ_TIMEOUT_SECONDS = ENV.fetch('GAMEINFO_HTTP_READ_TIMEOUT_SECONDS', 60).to_i
  MAX_ATTEMPTS = 3
  SEEN_TTL_SECONDS = 1.day.to_i

  # Battles are only listed once they are over, and never when they have fewer than 3 kills.
  BACKFILL_MIN_KILLS = 3
  BACKFILL_DELAY_SECONDS = 15.minutes.to_i
  BACKFILL_GIVE_UP_SECONDS = 1.hour.to_i

  PENDING_BATTLES_KEY = 'gameinfo:battles:pending'
  PENDING_BATTLE_KILLS_KEY = 'gameinfo:battles:pending:kills'

  RELEASE_LOCK_SCRIPT = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"

  def initialize(server_id)
    @server_id = server_id
    @redis = REDIS[server_id]
  end

  # Pages through the feed until a page has nothing new. A page with only new events means the
  # run is behind, so the next pages are then fetched EVENTS_PARALLEL_PAGES at a time.
  def poll_events
    with_lock('events') do
      published = 0
      offset = 0
      pages_at_once = 1
      done = false

      while !done && offset <= EVENTS_MAX_OFFSET && time_left?
        offsets = (0...pages_at_once).map { |i| offset + i * PAGE_SIZE }.select { |o| o <= EVENTS_MAX_OFFSET }
        pages = get_json_in_parallel(offsets.map { |o| "/api/gameinfo/events?limit=#{PAGE_SIZE}&offset=#{o}" })
        offset = offsets.last + PAGE_SIZE

        pages.each do |events|
          unless events.is_a?(Array) && events.any?
            done = true
            next
          end

          new_events = events.select { |event| publish_once(kill_event_key(event['EventId']), KILL_EVENTS_TOPIC, event) }
          new_events.each { |event| track_battle(event) }
          published += new_events.size
          done = true if new_events.empty?
          pages_at_once = EVENTS_PARALLEL_PAGES if new_events.size == events.size
        end
      end

      log('poll_events', published: published, offset: offset, timed_out: !time_left?)
      log('poll_events', message: 'every page up to the max offset had new events, older events were missed') if !done && offset > EVENTS_MAX_OFFSET
    end
  end

  def poll_battles
    with_lock('battles') do
      published = 0
      offset = 0

      while offset <= BATTLES_MAX_OFFSET && time_left?
        battles = get_json("/api/gameinfo/battles?sort=recent&limit=#{PAGE_SIZE}&offset=#{offset}")
        break unless battles.is_a?(Array) && battles.any?

        battles.each { |battle| published += 1 if publish_battle(battle) }
        offset += PAGE_SIZE
      end

      backfilled = backfill_battles
      log('poll_battles', published: published, backfilled: backfilled, timed_out: !time_left?)
    end
  end

  private

  # Fetches battles seen in kill events that the battles list never showed.
  def backfill_battles
    published = 0
    now = Time.now.to_i
    pending = @redis.zrangebyscore(PENDING_BATTLES_KEY, '-inf', now - BACKFILL_DELAY_SECONDS, with_scores: true)

    pending.each do |battle_id, last_kill_at|
      kills = @redis.hget(PENDING_BATTLE_KILLS_KEY, battle_id).to_i
      if kills < BACKFILL_MIN_KILLS || @redis.exists?(battle_key(battle_id))
        forget_battle(battle_id)
        next
      end

      break unless time_left?

      battle = get_json("/api/gameinfo/battles/#{battle_id}")
      if battle.is_a?(Hash)
        published += 1 if publish_battle(battle)
      elsif last_kill_at < now - BACKFILL_GIVE_UP_SECONDS
        forget_battle(battle_id)
      end
    end

    published
  end

  def publish_battle(battle)
    return false unless publish_once(battle_key(battle['id']), BATTLES_TOPIC, battle)

    forget_battle(battle['id'])
    true
  end

  def track_battle(event)
    battle_id = event['BattleId']
    return if battle_id.blank? || @redis.exists?(battle_key(battle_id))

    @redis.multi do |transaction|
      transaction.zadd(PENDING_BATTLES_KEY, Time.parse(event['TimeStamp']).to_i, battle_id, gt: true)
      transaction.hincrby(PENDING_BATTLE_KILLS_KEY, battle_id, 1)
    end
  end

  def forget_battle(battle_id)
    @redis.multi do |transaction|
      transaction.zrem(PENDING_BATTLES_KEY, battle_id)
      transaction.hdel(PENDING_BATTLE_KILLS_KEY, battle_id)
    end
  end

  def publish_once(key, topic, record)
    return false unless @redis.set(key, '1', nx: true, ex: SEEN_TTL_SECONDS)

    nats.send(topic, record.to_json)
    true
  rescue StandardError
    @redis.del(key)
    raise
  end

  # Only the HTTP calls run in threads; the results are handled in order on the job's thread.
  def get_json_in_parallel(paths)
    paths.map { |path| Thread.new { get_json(path) } }.map(&:value)
  end

  # Returns the parsed body, :not_found on 404, or nil when the request failed.
  # Rate limits (429), server errors and network errors are retried with a backoff.
  # The random `_` parameter skips the API's response cache, which serves each page URL
  # from a copy up to ~2 minutes old and makes consecutive pages come from different moments.
  def get_json(path)
    MAX_ATTEMPTS.times do |attempt|
      if attempt > 0
        break unless time_left?
        sleep(2**attempt)
      end

      response = HTTParty.get("#{BASE_URLS.fetch(@server_id)}#{path}",
                              query: { _: SecureRandom.hex(8) },
                              headers: { 'User-Agent' => 'AlbionDataServer' },
                              open_timeout: HTTP_OPEN_TIMEOUT_SECONDS,
                              read_timeout: HTTP_READ_TIMEOUT_SECONDS)
      return JSON.parse(response.body) if response.success?
      return :not_found if response.code == 404

      log('get_json', path: path, status: response.code)
      break unless response.code == 429 || response.code >= 500
    rescue StandardError => e
      log('get_json', path: path, error: e.message)
    end

    nil
  end

  # Only one run per job and server at a time. The lock is released when the run finishes
  # successfully, otherwise it expires after LOCK_TTL_SECONDS.
  def with_lock(name)
    key = "gameinfo:lock:#{name}"
    token = SecureRandom.uuid

    unless @redis.set(key, token, nx: true, ex: LOCK_TTL_SECONDS)
      log(name, message: 'previous run still holds the lock, skipping')
      return
    end

    @deadline = Time.now + MAX_RUNTIME_SECONDS
    yield
    @redis.eval(RELEASE_LOCK_SCRIPT, keys: [key], argv: [token])
  ensure
    @nats&.close
    @nats = nil
  end

  def time_left?
    Time.now < @deadline
  end

  def nats
    @nats ||= NatsService.new(@server_id)
  end

  def kill_event_key(event_id)
    "gameinfo:killevent:#{event_id}"
  end

  def battle_key(battle_id)
    "gameinfo:battle:#{battle_id}"
  end

  def log(method, data = {})
    Sidekiq.logger.info({ class: 'GameinfoService', method: method, server_id: @server_id }.merge(data).to_json)
  end
end
