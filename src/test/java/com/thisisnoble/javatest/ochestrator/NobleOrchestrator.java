package com.thisisnoble.javatest.ochestrator;

import java.util.LinkedList;

import com.thisisnoble.javatest.Event;
import com.thisisnoble.javatest.Orchestrator;
import com.thisisnoble.javatest.Processor;
import com.thisisnoble.javatest.Publisher;

// NobleOrchestrator thread-safe singleton class

public class NobleOrchestrator implements Orchestrator {
	// Note:
	// The volatile keyword now ensures that multiple threads running 
	// on different cores handle the singleton instance correctly. 
	
	private volatile static NobleOrchestrator instance;
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
		publisher.publish(event);
		for (Processor p: processors) {
			if (p.interestedIn(event)) {
				p.process(event);
				return;
			}
		}
		
	}

	@Override
	public void setup(Publisher publisher) {
		this.publisher = publisher;
	}
}
