package com.thisisnoble.javatest;

import com.thisisnoble.javatest.events.MarginEvent;
import com.thisisnoble.javatest.events.RiskEvent;
import com.thisisnoble.javatest.events.ShippingEvent;
import com.thisisnoble.javatest.events.TradeEvent;
import com.thisisnoble.javatest.impl.CompositeEvent;
import com.thisisnoble.javatest.processors.MarginProcessor;
import com.thisisnoble.javatest.processors.RiskProcessor;
import com.thisisnoble.javatest.processors.ShippingProcessor;

import org.junit.Test;

import static com.thisisnoble.javatest.util.TestIdGenerator.tradeEventId;
import static org.junit.Assert.*;

import com.thisisnoble.javatest.ochestrator.NobleOrchestrator;

public class SimpleOrchestratorTest {

	@Test
    public void tradeEventShouldTriggerAllProcessors() {
        TestPublisher testPublisher = new TestPublisher();
        Orchestrator orchestrator = setupOrchestrator();
        orchestrator.setup(testPublisher);

        TradeEvent te = new TradeEvent(tradeEventId(), 1000.0);
        orchestrator.receive(te);
        safeSleep(100);
        CompositeEvent ce = (CompositeEvent) testPublisher.getLastEvent();
        assertEquals(te, ce.getParent());
        assertEquals(5, ce.size());
        RiskEvent re1 = ce.getChildById("tradeEvt-riskEvt");
        assertNotNull(re1);
        assertEquals(50.0, re1.getRiskValue(), 0.01);
        MarginEvent me1 = ce.getChildById("tradeEvt-marginEvt");
        assertNotNull(me1);
        assertEquals(10.0, me1.getMargin(), 0.01);
        ShippingEvent se1 = ce.getChildById("tradeEvt-shipEvt");
        assertNotNull(se1);
        assertEquals(200.0, se1.getShippingCost(), 0.01);
        RiskEvent re2 = ce.getChildById("tradeEvt-shipEvt-riskEvt");
        assertNotNull(re2);
        assertEquals(10.0, re2.getRiskValue(), 0.01);
        MarginEvent me2 = ce.getChildById("tradeEvt-shipEvt-marginEvt");
        assertNotNull(me2);
        assertEquals(2.0, me2.getMargin(), 0.01);
        System.out.println("tradeEventShouldTriggerAllProcessors passed !");
    }

    @Test
    public void shippingEventShouldTriggerOnly2Processors() {
        TestPublisher testPublisher = new TestPublisher();
        Orchestrator orchestrator = setupOrchestrator();
        orchestrator.setup(testPublisher);

        ShippingEvent se = new ShippingEvent("ship2", 500.0);
        orchestrator.receive(se);
        safeSleep(100);
        CompositeEvent ce = (CompositeEvent) testPublisher.getLastEvent();
        assertEquals(se, ce.getParent());
        assertEquals(2, ce.size());
        RiskEvent re2 = ce.getChildById("ship2-riskEvt");
        assertNotNull(re2);
        assertEquals(25.0, re2.getRiskValue(), 0.01);
        MarginEvent me2 = ce.getChildById("ship2-marginEvt");
        assertNotNull(me2);
        assertEquals(5.0, me2.getMargin(), 0.01);
        System.out.println("shippingEventShouldTriggerOnly2Processors passed !");
    }
    
    // Test handling of ten thousand trade events sent by one thread
    // Expected result:
    // Ten thousand composite events each of which 
    // has one unique parent and five unique children
    @Test
    public void TenThousandTradeEvents() {
        TestPublisher testPublisher = new TestPublisher();
        Orchestrator orchestrator = setupOrchestrator();
        orchestrator.setup(testPublisher);
        assertTrue(testPublisher.getSize()==0);
        assertTrue(orchestrator.getNumOfProcessors()==3);
        
        String prefix = "trade";
        
        int numEvents = 10000;
        for (int i = 0; i<numEvents; i++) {
        	TradeEvent te = new TradeEvent(prefix+Integer.toString(i), i);
        	orchestrator.receive(te);
        }
        
        int size = testPublisher.getSize();
        int totalSleep = 0;
        while (size!=numEvents) {
        	safeSleep(500);
        	totalSleep = totalSleep + 500;
        	// Abort if we have slept more than sixty seconds
        	if (totalSleep>5000*60)
        		assertTrue(false);
        	size = testPublisher.getSize();
        }
        
        assertTrue(testPublisher.getSize()==numEvents);
        for (int i = 0; i<numEvents; i++) {
        	CompositeEvent ce = (CompositeEvent) testPublisher.getLastEvent();
        	String key = prefix+Integer.toString(i);
        	assertTrue(ce.getParent().getId().equals(key));
        	assertTrue(ce.size()==5);
        	assertTrue(ce.getChildById(key+"-riskEvt")!=null);
        	assertTrue(ce.getChildById(key+"-marginEvt")!=null);
        	assertTrue(ce.getChildById(key+"-shipEvt")!=null);
        	assertTrue(ce.getChildById(key+"-shipEvt-riskEvt")!=null);
        	assertTrue(ce.getChildById(key+"-shipEvt-marginEvt")!=null);
        }     
        assertTrue(testPublisher.getSize()==0);
        System.out.println("TenThousandTradeEvents passed !");
    }
    
    // Test handling of ten thousand shipping events sent by one thread
    // Expected result:
    // Ten thousand composite events each of which 
    // has one unique parent and two unique children
    @Test
    public void TenThousandShippingEvents() {
        TestPublisher testPublisher = new TestPublisher();
        Orchestrator orchestrator = setupOrchestrator();

        orchestrator.setup(testPublisher);
        assertTrue(testPublisher.getSize()==0);
        assertTrue(orchestrator.getNumOfProcessors()==3);
        
        String prefix = "ship";
        
        int numEvents = 10000;
        for (int i = 0; i<numEvents; i++) {
        	ShippingEvent se = new ShippingEvent(prefix+Integer.toString(i), i);
        	orchestrator.receive(se);
        }
        
        int size = testPublisher.getSize();
        int totalSleep = 0;
        while (size!=numEvents) {
        	safeSleep(500);
        	totalSleep = totalSleep + 500;
        	// Abort if we have slept more than sixty seconds
        	if (totalSleep>5000*60)
        		assertTrue(false);
        	size = testPublisher.getSize();
        }
        
        assertTrue(testPublisher.getSize()==numEvents);
        for (int i = 0; i<numEvents; i++) {
        	CompositeEvent ce = (CompositeEvent) testPublisher.getLastEvent();
        	String key = prefix+Integer.toString(i);
        	assertTrue(ce.getParent().getId().equals(key));
        	assertTrue(ce.size()==2);
        	assertTrue(ce.getChildById(key+"-riskEvt")!=null);
        	assertTrue(ce.getChildById(key+"-marginEvt")!=null);
        }
        assertTrue(testPublisher.getSize()==0);
        System.out.println("TenThousandShippingEvents passed !");
    }

    private Runnable createShippingEvent(final Orchestrator orchestrator, final String prefix, final int size){
    	
        Runnable aRunnable = new Runnable(){
            public void run(){                
                for (int i = 0; i<size; i++) {
                	ShippingEvent se = new ShippingEvent(prefix+Integer.toString(i), i);
                	orchestrator.receive(se);
                }
            }
        };
        return aRunnable;
    }
    
    private Runnable createTradeEvent(final Orchestrator orchestrator, final String prefix, final int size){

        Runnable aRunnable = new Runnable(){
            public void run(){
                for (int i = 0; i<size; i++) {
                	TradeEvent te = new TradeEvent(prefix+Integer.toString(i), i);
                	orchestrator.receive(te);
                }
            }
        };
        return aRunnable;
    }

    // Test handling of 20K shipping events and 20K trade events injected
    // by four different threads
    // Expected result:
    // Forty thousand composite events
    // Each composite event of a ShipEvent parent has two unique children
    // Each composite event of a TradeEven parent has five unique children
    @Test
    public void fortyThousandEventsInjectedByFourThreads() {
        TestPublisher testPublisher = new TestPublisher();
        Orchestrator orchestrator = setupOrchestrator();

        orchestrator.setup(testPublisher);
        assertTrue(testPublisher.getSize()==0);
        assertTrue(orchestrator.getNumOfProcessors()==3);
        
        int numEvents = 10000;
        int numThreads = 4;
        
        Thread t1 = new Thread(createShippingEvent(orchestrator, "set1", numEvents));
        Thread t2 = new Thread(createShippingEvent(orchestrator, "set2", numEvents));        
        Thread t3 = new Thread(createTradeEvent(orchestrator, "tet3", numEvents));
        Thread t4 = new Thread(createTradeEvent(orchestrator, "tet4", numEvents));           
        
        t1.start();
        t2.start();
        t3.start();
        t4.start();
        
        int size = testPublisher.getSize();
        int totalSleep = 0;
        int numCompositeEvents = numThreads*numEvents;
        
        while (size!=numCompositeEvents) {
        	safeSleep(500);
        	totalSleep = totalSleep + 500;
        	// Abort if we have slept more than sixty seconds
        	if (totalSleep>5000*60)
        		assertTrue(false);
        	size = testPublisher.getSize();
        }
   
        assertTrue(testPublisher.getSize()==numCompositeEvents);
                
        // Verify all the parents and children
        for (int i = 0; i<numCompositeEvents; i++) {
        	CompositeEvent ce = (CompositeEvent) testPublisher.getLastEvent();
        	String key = ce.getParent().getId();
        	if (key.startsWith("set1")||key.startsWith("set2")) {
	        	assertTrue(ce.getParent().getId().equals(key));
	        	assertTrue(ce.size()==2);
	        	assertTrue(ce.getChildById(key+"-riskEvt")!=null);
	        	assertTrue(ce.getChildById(key+"-marginEvt")!=null);
        	}
        	if (key.startsWith("tet3")||key.startsWith("tet4")) {
	        	assertTrue(ce.getParent().getId().equals(key));
	        	assertTrue(ce.size()==5);
	        	assertTrue(ce.getChildById(key+"-riskEvt")!=null);
	        	assertTrue(ce.getChildById(key+"-marginEvt")!=null);
	        	assertTrue(ce.getChildById(key+"-shipEvt")!=null);
	        	assertTrue(ce.getChildById(key+"-shipEvt-riskEvt")!=null);
	        	assertTrue(ce.getChildById(key+"-shipEvt-marginEvt")!=null);
        	}
        }
    
        assertTrue(testPublisher.getSize()==0);
        System.out.println("FortyThousandEventsInjectedByFourThreads passed !");
    }
    
    private Orchestrator setupOrchestrator() {
        Orchestrator orchestrator = createOrchestrator();
        ((NobleOrchestrator)orchestrator).deleteAllProcessors();
        ((NobleOrchestrator)orchestrator).clearCompositeEventMap();
        orchestrator.register(new RiskProcessor(orchestrator));
        orchestrator.register(new MarginProcessor(orchestrator));
        orchestrator.register(new ShippingProcessor(orchestrator));
        return orchestrator;
    }

    private void safeSleep(long l) {
        try {
            Thread.sleep(l);
        } catch (InterruptedException e) {
            //ignore
        }
    }

    private Orchestrator createOrchestrator() {
    	return NobleOrchestrator.getInstance();
    }
}
