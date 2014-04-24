package com.thisisnoble.javatest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.thisisnoble.javatest.impl.CompositeEvent;

public class TestPublisher implements Publisher {

    private Object listLock = new Object();
    
    final List<CompositeEvent> list = Collections.synchronizedList(new ArrayList<CompositeEvent>());
    
    @Override
    public void publish(Event event) {
        list.add((CompositeEvent)event);
    }

    public Event getLastEvent() {
    	// We need to synchronize here to avoid the situation
    	// where there is only one element in the list and
    	// multiple threads trying to remove that same element
    	synchronized(listLock) {
    		CompositeEvent ce = null;
    		if (list.size()>0)
    			ce = list.remove(0);
    		return ce;
    	}
    }
}
