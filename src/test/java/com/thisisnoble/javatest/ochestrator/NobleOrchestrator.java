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
	Object biglock = new Object();
	
	private void createCompositeEventAndAddParent(Event event) {
		synchronized(event) {
			String key = event.getId();
			if (compositeEventMap.get(key)==null) {
				CompositeEvent ce = new CompositeEvent(key, event);
				System.out.println("add Parent: key = "+key);
				compositeEventMap.put(key, ce);
			}
		}
	}
	
	private void addChild(String key, Event event) {
		synchronized(biglock) {
			CompositeEvent ce = compositeEventMap.get(key);
			assert(ce!=null);
			System.out.println("add child: key = "+key+"child id = "+event.getId());
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
		synchronized(biglock) {
			String eventId = event.getId();
			int idx = eventId.indexOf('-');
			assert(idx>1);
			//String key = idx==-1 ? eventId:eventId.substring(0,idx-1);
			String key = eventId.substring(0,idx);
			return key;
		}
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
			createCompositeEventAndAddParent(event);
		}
		else 
		{
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
