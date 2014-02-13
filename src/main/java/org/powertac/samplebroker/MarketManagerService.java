/*
 * Copyright (c) 2012-2013 by the original author
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.powertac.samplebroker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;
import org.powertac.common.BalancingTransaction;
import org.powertac.common.ClearedTrade;
import org.powertac.common.Competition;
import org.powertac.common.CustomerInfo;
import org.powertac.common.DistributionTransaction;
import org.powertac.common.MarketPosition;
import org.powertac.common.MarketTransaction;
import org.powertac.common.Order;
import org.powertac.common.Orderbook;
import org.powertac.common.OrderbookOrder;
import org.powertac.common.Timeslot;
import org.powertac.common.WeatherForecast;
import org.powertac.common.WeatherReport;
import org.powertac.common.config.ConfigurableValue;
import org.powertac.common.msg.MarketBootstrapData;
import org.powertac.common.repo.TimeslotRepo;
import org.powertac.common.repo.CustomerRepo;
import org.powertac.samplebroker.PortfolioManagerService.CustomerRecord;
import org.powertac.samplebroker.core.BrokerPropertiesService;
import org.powertac.samplebroker.interfaces.Activatable;
import org.powertac.samplebroker.interfaces.BrokerContext;
import org.powertac.samplebroker.interfaces.Initializable;
import org.powertac.samplebroker.interfaces.MarketManager;
import org.powertac.samplebroker.interfaces.PortfolioManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Handles market interactions on behalf of the broker.
 * @author John Collins
 */
@Service
public class MarketManagerService 
implements MarketManager, Initializable, Activatable
{
  static private Logger log = Logger.getLogger(MarketManagerService.class);
  
  private BrokerContext broker; // broker

  // Spring fills in Autowired dependencies through a naming convention
  @Autowired
  private BrokerPropertiesService propertiesService;

  @Autowired
  private TimeslotRepo timeslotRepo;
  
  @Autowired
  private PortfolioManager portfolioManager;

  // ------------ Configurable parameters --------------
  // max and min offer prices. Max means "sure to trade"
  @ConfigurableValue(valueType = "Double",
          description = "Upper end (least negative) of bid price range")
  private double buyLimitPriceMax = -1.0;  // broker pays

  @ConfigurableValue(valueType = "Double",
          description = "Lower end (most negative) of bid price range")
  private double buyLimitPriceMin = -70.0;  // broker pays

  @ConfigurableValue(valueType = "Double",
          description = "Upper end (most positive) of ask price range")
  private double sellLimitPriceMax = 70.0;    // other broker pays

  @ConfigurableValue(valueType = "Double",
          description = "Lower end (least positive) of ask price range")
  private double sellLimitPriceMin = 0.5;    // other broker pays

  @ConfigurableValue(valueType = "Double",
          description = "Minimum bid/ask quantity in MWh")
  private double minMWh = 0.001; // don't worry about 1 KWh or less

  // ---------------- local state ------------------
  private Random randomGen = new Random(); // to randomize bid/ask prices

  // Bid recording
  private HashMap<Integer, Order> lastOrder;
  private double[] marketMWh;
  private double[] marketPrice;
  private double meanMarketPrice = 0.0;

  //private HashMap<Integer, ArrayList<MarketTransaction>> marketTxMap;
  private ArrayList<WeatherReport> weather;
  private ArrayList<Orderbook> OrderBook;

  public MarketManagerService ()
  {
    super();
  }

  /* (non-Javadoc)
   * @see org.powertac.samplebroker.MarketManager#init(org.powertac.samplebroker.SampleBroker)
   */
  @SuppressWarnings("unchecked")
  @Override
  public void initialize (BrokerContext broker)
  {
    this.broker = broker;
    lastOrder = new HashMap<Integer, Order>();
    propertiesService.configureMe(this);
    //marketTxMap = new HashMap<Integer, ArrayList<MarketTransaction>>();
    weather = new ArrayList<WeatherReport>();
    for (Class<?> messageType: Arrays.asList(BalancingTransaction.class,
                                             ClearedTrade.class,
                                             DistributionTransaction.class,
                                             MarketBootstrapData.class,
                                             MarketPosition.class,
                                             MarketTransaction.class,
                                             Orderbook.class,
                                             WeatherForecast.class,
                                             WeatherReport.class)) {
      broker.registerMessageHandler(this, messageType);
    }
  }
  
  // ----------------- data access -------------------
  /**
   * Returns the mean price observed in the market
   */
  @Override
  public double getMeanMarketPrice ()
  {
    return meanMarketPrice;
  }
  
  // --------------- message handling -----------------

  /**
   * Handles a BalancingTransaction message.
   */
  public void handleMessage (BalancingTransaction tx)
  {
    log.info("Balancing tx: " + tx.getCharge());
  }

  /**
   * Handles a ClearedTrade message - this is where you would want to keep
   * track of market prices.
   */
  public void handleMessage (ClearedTrade ct)
  {
  }
  
  /**
   * Handles a DistributionTransaction - charges for transporting power
   */
  public void handleMessage (DistributionTransaction dt)
  {
    log.info("Distribution tx: " + dt.getCharge());
  }

  /**
   * Receives a MarketBootstrapData message, reporting usage and prices
   * for the bootstrap period. We record the overall weighted mean price,
   * as well as the mean price and usage for a week.
   */
  public void handleMessage (MarketBootstrapData data)
  {
    marketMWh = new double[broker.getUsageRecordLength()];
    marketPrice = new double[broker.getUsageRecordLength()];
    double totalUsage = 0.0;
    double totalValue = 0.0;
    for (int i = 0; i < data.getMwh().length; i++) {
      totalUsage += data.getMwh()[i];
      totalValue += data.getMarketPrice()[i] * data.getMwh()[i];
      if (i < broker.getUsageRecordLength()) {
        // first pass, just copy the data
        marketMWh[i] = data.getMwh()[i];
        marketPrice[i] = data.getMarketPrice()[i];
      }
      else {
        // subsequent passes, accumulate mean values
        int pass = i / broker.getUsageRecordLength();
        int index = i % broker.getUsageRecordLength();
        marketMWh[index] =
            (marketMWh[index] * pass + data.getMwh()[i]) / (pass + 1);
        marketPrice[index] =
            (marketPrice[index] * pass + data.getMarketPrice()[i]) / (pass + 1);
      }
    }
    meanMarketPrice = totalValue / totalUsage;
  }

  /**
   * Receives a MarketPosition message, representing our commitments on 
   * the wholesale market
   */
  public void handleMessage (MarketPosition posn)
  {
    broker.getBroker().addMarketPosition(posn, posn.getTimeslotIndex());
  }
  
  /**
   * Receives a new MarketTransaction. We look to see whether an order we
   * have placed has cleared.
   */
  public void handleMessage (MarketTransaction tx)
  {
    // reset price escalation when a trade fully clears.
    Order lastTry = lastOrder.get(tx.getTimeslotIndex());
    if (lastTry == null) // should not happen
      log.error("order corresponding to market tx " + tx + " is null");
    else if (tx.getMWh() == lastTry.getMWh()){ // fully cleared
    	lastOrder.put(tx.getTimeslotIndex(), null);
    }
  }
  
  /**
   * Receives the market orderbooks
   */
  public void handleMessage (Orderbook orderbook)
  {
	  OrderBook.add(orderbook);
  }
  
  //Return the list which contains orderbook
  public Orderbook getOrderBook(int timeslotIndex)
  {
	  return OrderBook.get(timeslotIndex);
  }
  
  /**
   * Receives a new WeatherForecast.
   */
  public void handleMessage (WeatherForecast forecast)
  {
  }

  /**
   * Receives a new WeatherReport.
   */
  public void handleMessage (WeatherReport report)
  {
	  weather.add(report);
  }
  
  //Return the list which contains all weather reports
  public WeatherReport getWeatherReport(int timeslotIndex)
  {
	  return weather.get(timeslotIndex);
  }
  

  // ----------- per-timeslot activation ---------------

  /**
   * Compute needed quantities for each open timeslot, then submit orders
   * for those quantities.
   * @see org.powertac.samplebroker.MarketManager#activate()
   */
  @Override
  public void activate (int timeslotIndex)
  {
    double neededKWh = 0.0;
    log.debug("Current timeslot is " + timeslotRepo.currentTimeslot().getSerialNumber());
    for (Timeslot timeslot : timeslotRepo.enabledTimeslots()) {
      int index = (timeslot.getSerialNumber()) % broker.getUsageRecordLength();
      neededKWh = portfolioManager.collectUsage(index);
      submitOrder2(neededKWh, timeslot.getSerialNumber());
    }
  }

  /**
   * Composes and submits the appropriate order for the given timeslot.
   */
  private void submitOrder (double neededKWh, int timeslot)
  {
    double neededMWh = neededKWh / 1000.0;

    MarketPosition posn =
        broker.getBroker().findMarketPositionByTimeslot(timeslot);
    if (posn != null)
      neededMWh -= posn.getOverallBalance();
    log.debug("needed mWh=" + neededMWh +
              ", timeslot " + timeslot);
    if (Math.abs(neededMWh) <= minMWh) {
      log.info("no power required in timeslot " + timeslot);
      return;
    }
    Double limitPrice = computeLimitPrice(timeslot, neededMWh);
    log.info("new order for " + neededMWh + " at " + limitPrice +
             " in timeslot " + timeslot);
    Order order = new Order(broker.getBroker(), timeslot, neededMWh, limitPrice);
    lastOrder.put(timeslot, order);
    broker.sendMessage(order);
  }
  
  private void submitOrder2 (double neededKWh, int timeslot)
  {
    double neededMWh = neededKWh / 1000.0;
    int CurrentTimeSlot = timeslotRepo.currentSerialNumber();

    MarketPosition posn =
        broker.getBroker().findMarketPositionByTimeslot(timeslot);
    if (posn != null)
      neededMWh -= posn.getOverallBalance();
    log.debug("needed mWh=" + neededMWh +
              ", timeslot " + timeslot);
    if (Math.abs(neededMWh) <= minMWh) {
      log.info("no power required in timeslot " + timeslot);
      return;
    }
    Double limitPrice = computeLimitPrice(timeslot, neededMWh);
    log.info("new order for " + neededMWh + " at " + limitPrice +
             " in timeslot " + timeslot);
    
    
    Order order;
	System.out.println("Current TimeSlot:"+CurrentTimeSlot);
	System.out.println("Order for TimeSlot:"+timeslot);
	
	int count1=0;
	int count2=0;
	double AVG_Solar_Sunny=0;
	double AVG_Solar_Cloudy=0;
	int count3=0;
	int count4=0;
	double AVG_Wind_Windy=0;
	double AVG_Wind_No_Windy=0;
	
	/*See the previous slots how much was the AVG solar power based on weather 
	 * we see only few slots before so the number of our customers-producers will be the same*/
	if (CurrentTimeSlot>350){
		for (int i=24;i<35;i++){
			double WP = getWeatherReport(CurrentTimeSlot-i).getCloudCover();
			if (WP>0.5){
				AVG_Solar_Cloudy += portfolioManager.getSolarEnergy(CurrentTimeSlot-i);
				count2++;
			}
			else{
				AVG_Solar_Sunny+= portfolioManager.getSolarEnergy(CurrentTimeSlot-i);
				count1++;
			}
		}
		if (count1>0){
			AVG_Solar_Sunny=AVG_Solar_Sunny/count1;
		}
		if (count2>0){
			AVG_Solar_Cloudy=AVG_Solar_Cloudy/count2;
		}
		
		System.out.println("AVG Solar Energy Sunny Day:"+AVG_Solar_Sunny);
		System.out.println("AVG Solar Energy Cloudy Day:"+AVG_Solar_Cloudy);
		
		for (int i=24;i<35;i++){
			double WP = getWeatherReport(CurrentTimeSlot-i).getWindSpeed();
			if (WP>2.5){
				AVG_Wind_Windy += portfolioManager.getWindEnergy(CurrentTimeSlot-i);
				count3++;
			}
			else{
				AVG_Wind_No_Windy+= portfolioManager.getWindEnergy(CurrentTimeSlot-i);
				count4++;
			}
		}
		if (count3>0){
			AVG_Wind_Windy=AVG_Wind_Windy/count3;
		}
		if (count4>0){
			AVG_Wind_No_Windy=AVG_Wind_No_Windy/count4;
		}
		
		System.out.println("AVG Wind Energy Windy Day:"+AVG_Wind_Windy);
		System.out.println("AVG Wind Energy Tranquil Day:"+AVG_Wind_No_Windy);
	}
	
	
	double CustomerStorageCapacity = portfolioManager.getTotalStorage(timeslot);
	neededMWh=+CustomerStorageCapacity*0.4;
	System.out.println("Extra buy:"+CustomerStorageCapacity*0.4);
	
    if (CurrentTimeSlot==timeslot){
    	order = new Order(broker.getBroker(), timeslot, neededMWh, limitPrice);
    	System.out.println("Buying for the current timeslot");
    }
    else{
    	System.out.println("Buying for future timeslot");
    	 	if (neededMWh>0){			//If we want to buy
    	    	//order = new Order(broker.getBroker(), timeslot, neededMWh, limitPrice*(1-discount));
	    		order = new Order(broker.getBroker(), timeslot, neededMWh, limitPrice);
	 	    	System.out.println("Buy Energy");
	    	}
    	    else						//if we want to sell
    	    {
    	    	System.out.println("Sell Energy");
    	    	CustomerStorageCapacity = portfolioManager.getTotalStorage(timeslot);	
    	    	//We always buy more energy 23 slots ahead. The extra energy is equal to the 40% of the storage
    	    	if (timeslotRepo.currentTimeslot().getSerialNumber()!= timeslot+23){
    	    		CustomerStorageCapacity=0;
    	    	}
    	    	
    	    	order = new Order(broker.getBroker(), timeslot, neededMWh, limitPrice);
    	    }
    }
    
   
    lastOrder.put(timeslot, order);
    broker.sendMessage(order);
  }

  /**
   * Computes a limit price with a random element. 
   */
  private Double computeLimitPrice (int timeslot,
                                    double amountNeeded)
  {
    log.debug("Compute limit for " + amountNeeded + 
              ", timeslot " + timeslot);
    // start with default limits
    Double oldLimitPrice;
    double minPrice;
    if (amountNeeded > 0.0) {
      // buying
      oldLimitPrice = buyLimitPriceMax;
      minPrice = buyLimitPriceMin;
    }
    else {
      // selling
      oldLimitPrice = sellLimitPriceMax;
      minPrice = sellLimitPriceMin;
    }
    // check for escalation
    Order lastTry = lastOrder.get(timeslot);
    if (lastTry != null)
      log.debug("lastTry: " + lastTry.getMWh() +
                " at " + lastTry.getLimitPrice());
    if (lastTry != null
        && Math.signum(amountNeeded) == Math.signum(lastTry.getMWh())) {
      oldLimitPrice = lastTry.getLimitPrice();
      log.debug("old limit price: " + oldLimitPrice);
    }

    // set price between oldLimitPrice and maxPrice, according to number of
    // remaining chances we have to get what we need.
    double newLimitPrice = minPrice; // default value
    int current = timeslotRepo.currentSerialNumber();
    int remainingTries = (timeslot - current
                          - Competition.currentCompetition().getDeactivateTimeslotsAhead());
    log.debug("remainingTries: " + remainingTries);
    if (remainingTries > 0) {
      double range = (minPrice - oldLimitPrice) * 2.0 / (double)remainingTries;
      log.debug("oldLimitPrice=" + oldLimitPrice + ", range=" + range);
      double computedPrice = oldLimitPrice + randomGen.nextDouble() * range; 
      return Math.max(newLimitPrice, computedPrice);
    }
    else
      return null; // market order
  }
}
