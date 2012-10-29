require './lib/multi_db/scheduler'

describe MultiDb::Scheduler do

  before do
    @items = [5, 7, 4, 8]
    @scheduler = MultiDb::Scheduler.new(@items.clone)
  end

  it "should return items in a round robin fashion" do
    first = @items.shift
    @scheduler.next until @scheduler.current == first
    @scheduler.current.should == first
    @items.each do |item|
      @scheduler.next.should == item
    end
    @scheduler.next.should == first
  end

  it 'should not return blacklisted items' do
    @scheduler.blacklist!(4)
    @items.size.times do
      @scheduler.next.should_not == 4
    end
  end

  it 'should raise NoMoreItems if all are blacklisted' do
    @items.each do |item|
      @scheduler.blacklist!(item)
    end
    lambda {
      @scheduler.next
    }.should raise_error(MultiDb::Scheduler::NoMoreItems)
  end

  it 'should unblacklist items automatically' do
    @scheduler = MultiDb::Scheduler.new(@items.clone, 0)
    @scheduler.blacklist!(7)
    sleep(1)
    @scheduler.next.should == 7
  end

  describe '(accessed from multiple threads)' do

    it '#current and #next should return the same item for the same thread' do
      @scheduler.next until @scheduler.current == @items.first
      3.times {
        Thread.new do
          @scheduler.current.should == 5
          @scheduler.next.should == 7
        end.join
      }
      @scheduler.next.should == 7
    end

  end

end

