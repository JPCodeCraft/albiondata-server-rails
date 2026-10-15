require 'rails_helper'

RSpec.describe KillEventPollWorker, type: :worker do
  it 'calls GameinfoService.poll_events' do
    s = double
    allow(GameinfoService).to receive(:new).with('west').and_return(s)
    expect(s).to receive(:poll_events)

    subject.perform('west')
  end
end
