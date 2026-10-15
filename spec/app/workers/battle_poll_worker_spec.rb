require 'rails_helper'

RSpec.describe BattlePollWorker, type: :worker do
  it 'calls GameinfoService.poll_battles' do
    s = double
    allow(GameinfoService).to receive(:new).with('west').and_return(s)
    expect(s).to receive(:poll_battles)

    subject.perform('west')
  end
end
