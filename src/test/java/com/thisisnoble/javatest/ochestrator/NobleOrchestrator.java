package com.thisisnoble.javatest.ochestrator;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.thisisnoble.javatest.Event;
import com.thisisnoble.javatest.Orchestrator;
import com.thisisnoble.javatest.Processor;
import com.thisisnoble.javatest.Publisher;
import com.thisisnoble.javatest.events.MarginEvent;
import com.thisisnoble.javatest.events.RiskEvent;
import com.thisisnoble.javatest.events.ShippingEvent;
import com.thisisnoble.javatest.events.TradeEvent;
import com.thisisnoble.javatest.impl.CompositeEvent;
import com.thisisnoble.javatest.util.TestIdGenerator;

// NobleOrchestrator thread-safe singleton class

public class NobleOrchestrator implements Orchestrator {
	// Note:
	// The volatile keyword now ensures that multiple threads running 
	// on different cores handle the singleton instance correctly. 
	private volatile static NobleOrchestrator instance;
	
	private Map<String, CompositeEvent> compositeEventMap = new ConcurrentHashMap<String, CompositeEvent>();
	
	private void createCompositeEventAndAddParent(Event event) {
		// Synchronizing on the event
		synchronized(event) {
			String key = event.getId();
			if (compositeEventMap.get(key)==null) {
				CompositeEvent ce = new CompositeEvent(key, event);
				System.out.println("add parent: key = "+key+" parent id = "+event.getId());
				compositeEventMap.put(key, ce);
			}
		}
	}
	
	private void addChild(String key, Event event) {
		CompositeEvent ce = compositeEventMap.get(key);
		assert(ce!=null);
		System.out.println("add child:  key = "+key+" child id = "+event.getId());
		// Synchronizing on the composite event
		synchronized(ce) {
			ce.addChild(event);
			Event parent = ce.getParent();
			int numChildren = ce.size();
			
			if (parent instanceof ShippingEvent && numChildren == 2) {
				publisher.publish(ce);
			}
			
			if (parent instanceof TradeEvent && numChildren == 5) {
				publisher.publish(ce);
			}
		}
	}
		
	private String getCompositeEventKey(Event event) {
			String eventId = event.getId();
			int idx = eventId.indexOf('-');
			assert(idx>1);
			//String key = idx==-1 ? eventId:eventId.substring(0,idx-1);
			String key = eventId.substring(0,idx);
			return key;
	}
	
	private NobleOrchestrator() {}	
	public static NobleOrchestrator getInstance() {
		// Double-checked locking to ensure only one NobleOchestrator instance is created
		if (instance == null) {
			synchronized(NobleOrchestrator.class) {
				if (instance == null) {
					instance = new NobleOrchestrator();
				}
			}
		}
		return instance;
	}
	
	private LinkedList<Processor> processors = new LinkedList<Processor>();
	private Publisher publisher = null;
	
	@Override
	public void register(Processor processor) {
		processors.add(processor);
	}

	@Override
	public void receive(Event event) {

		if (event.getId().indexOf('-')==-1) {
			// the event is received from outside
			createCompositeEventAndAddParent(event);
		}
		else 
		{
			// the event is received from one of the processors
			String key = getCompositeEventKey(event);
			assert(key!=null);
			addChild(key, event);
		}
		
		for (Processor p: processors) {
			if (p.interestedIn(event)) {
				p.process(event);
			}
		}
	}

	@Override
	public void setup(Publisher publisher) {
		this.publisher = publisher;
	}
}
