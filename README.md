Noble Group Programming Test (design and implemention)

1. The NobleOrchestrator is implemented as a thread-safe (double-checked locking) singleton class. 

2. The CompositeEvents are stored in a ConcurrentHashMap inside the NobleOrchestrator class. 
   I chose ConcurrentHashMap for relatively high throughput and thread-safe processing. Upon 
   completing processing all of the children events of a CompositeEvent, the CompositeEvent 
   will be published.

3. To ensure thread-safe processing over a CompositeEvent by different threads, the method to 
   create a composite event and add the parent to the composite event, and the method to add 
   children are synchronized. The former is synchronized on the parent event, and latter is 
   synchronized on the composite event.

4. Make TestPublisher thread-safe by using synchronizedList()

5. Three bugs were found and fixed in the original code: 
   The ShippingProcess was returning MarginEvent (should return ShippingEvent instead). 
   The two assert statements at the very end of shippingEventShouldTriggerOnly2Processors() 
   were asserting wrong risk value and margin. 

6. I added three new JUnit test cases:

   tenThousandShippingEvents()
   
   tenThousandTradeEvents()
   
   fortyThousandEventsInjectedByFourThreads()

7. The full tarball can be downloaded at:
   https://github.com/chicheongweng/noble
   To run the test, do mvn test
