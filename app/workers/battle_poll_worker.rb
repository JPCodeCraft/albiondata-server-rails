class BattlePollWorker
  include Sidekiq::Job
  # The next scheduled run is the retry.
  sidekiq_options retry: false

  def perform(server_id)
    GameinfoService.new(server_id).poll_battles
  end
end
