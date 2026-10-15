class KillEventPollWorker
  include Sidekiq::Job
  # The next scheduled run is the retry.
  sidekiq_options retry: false

  def perform(server_id)
    GameinfoService.new(server_id).poll_events
  end
end
