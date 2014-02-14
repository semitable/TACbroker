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

import org.apache.log4j.Logger;
import org.joda.time.Instant;
import org.powertac.common.Broker;
import org.powertac.common.Competition;
import org.powertac.common.CustomerInfo;
import org.powertac.common.Rate;
import org.powertac.common.Tariff;
import org.powertac.common.TariffSpecification;
import org.powertac.common.TariffTransaction;
import org.powertac.common.TimeService;
import org.powertac.common.WeatherReport;
import org.powertac.common.config.ConfigurableValue;
import org.powertac.common.enumerations.PowerType;
import org.powertac.common.msg.BalancingControlEvent;
import org.powertac.common.msg.BalancingOrder;
import org.powertac.common.msg.CustomerBootstrapData;
import org.powertac.common.msg.TariffRevoke;
import org.powertac.common.msg.TariffStatus;
import org.powertac.common.repo.CustomerRepo;
import org.powertac.common.repo.TariffRepo;
import org.powertac.common.repo.TimeslotRepo;
import org.powertac.samplebroker.core.BrokerPropertiesService;
import org.powertac.samplebroker.interfaces.Activatable;
import org.powertac.samplebroker.interfaces.BrokerContext;
import org.powertac.samplebroker.interfaces.Initializable;
import org.powertac.samplebroker.interfaces.MarketManager;
import org.powertac.samplebroker.interfaces.PortfolioManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.powertac.common.TariffEvaluationHelper;
/**
 * Handles portfolio-management responsibilities for the broker. This
 * includes composing and offering tariffs, keeping track of customers and their
 * usage, monitoring tariff offerings from competing brokers.
 * 
 * A more complete broker implementation might split this class into two or
 * more classes; the keys are to decide which messages each class handles,
 * what each class does on the activate() method, and what data needs to be
 * managed and shared.
 * 
 * @author John Collins
 */
@Service // Spring creates a single instance at startup
public class PortfolioManagerService 
implements PortfolioManager, Initializable, Activatable
{
  static private Logger log = Logger.getLogger(PortfolioManagerService.class);
  
  private BrokerContext brokerContext; // master

  // Spring fills in Autowired dependencies through a naming convention
  @Autowired
  private BrokerPropertiesService propertiesService;

  @Autowired
  private TimeslotRepo timeslotRepo;

  @Autowired
  private TariffRepo tariffRepo;

  @Autowired
  private CustomerRepo customerRepo;

  @Autowired
  private MarketManager marketManager;

  @Autowired
  private TimeService timeService;

  // ---- Portfolio records -----
  // Customer records indexed by power type and by tariff. Note that the
  // CustomerRecord instances are NOT shared between these structures, because
  // we need to keep track of subscriptions by tariff.
  private HashMap<PowerType,
                  HashMap<CustomerInfo, CustomerRecord>> customerProfiles;
  private HashMap<TariffSpecification, 
                  HashMap<CustomerInfo, CustomerRecord>> customerSubscriptions;
  private HashMap<PowerType, List<TariffSpecification>> competingTariffs;

  private HashMap<PowerType, List<TariffSpecification>> ourTariffs;
  // Configurable parameters for tariff composition
  // Override defaults in src/main/resources/config/broker.config
  // or in top-level config file
  @ConfigurableValue(valueType = "Double",
          description = "target profit margin")
  private double defaultMargin = 0.5;

  @ConfigurableValue(valueType = "Double",
          description = "Fixed cost/kWh")
  private double fixedPerKwh = -0.06;

  @ConfigurableValue(valueType = "Double",
          description = "Default daily meter charge")
  private double defaultPeriodicPayment = -1.0;
  
  @ConfigurableValue(valueType = "Double",
          description = "Subscription Payment")
  private double defaultSubscriptionPayment = -7.0;
  
  @ConfigurableValue(valueType = "Double",
          description = "Default Early withdrawal penalty")
  private double defaultEarlyWithdraw = -25.0;
  @ConfigurableValue(valueType = "Long",
          description = "Default Minimum Duration")
  private long defaultMinDuration = 600;
  

  /**
   * Default constructor registers for messages, must be called after 
   * message router is available.
   */
  public PortfolioManagerService ()
  {
    super();
  }

  /**
   * Per-game initialization. Configures parameters and registers
   * message handlers.
   */
  @Override // from Initializable
  @SuppressWarnings("unchecked")
  public void initialize (BrokerContext context)
  {
    this.brokerContext = context;
    propertiesService.configureMe(this);
    customerProfiles = new HashMap<PowerType,
        HashMap<CustomerInfo, CustomerRecord>>();
    customerSubscriptions = new HashMap<TariffSpecification,
        HashMap<CustomerInfo, CustomerRecord>>();
    competingTariffs = new HashMap<PowerType, List<TariffSpecification>>();
    ourTariffs = new HashMap<PowerType, List<TariffSpecification>>();
    for (Class<?> messageType: Arrays.asList(CustomerBootstrapData.class,
                                             TariffSpecification.class,
                                             TariffStatus.class,
                                             TariffTransaction.class,
                                             TariffRevoke.class,
                                             BalancingControlEvent.class)) {
      context.registerMessageHandler(this, messageType);
    }
  }
  //----------------printing---------------------
  @SuppressWarnings("unused")
private void printTariffRepo(){
		for (TariffSpecification spec :
	        tariffRepo.findAllTariffSpecifications()) {
			printTariff(spec);
		}
	}
	private void printTariff(TariffSpecification spec){
		System.out.println("--Tariff Spec--");
		System.out.println("ID: " + spec.getId());
		System.out.println("Offered By: " + spec.getBroker());
		System.out.println("Rate: " + spec.getRates());
		System.out.println("PowerType: " + spec.getPowerType());
		System.out.println("--------------");

	}
	@SuppressWarnings("unused")
	private void printTx(TariffTransaction tx)
	{
		System.out.println("--Tariff Transaction--");
		System.out.println("Tx Type: "+ tx.getTxType());
		System.out.println("Population Type: " + tx.getCustomerInfo());
		System.out.println("Population Count: " + tx.getCustomerCount());
		System.out.println("---------------------");
		
	}
	private void printTimeSlot(){
		System.out.println("====================");
	    System.out.println("TimeSlot: " + timeslotRepo.currentTimeslot().getSerialNumber());
	    System.out.println("====================");
	}
  // -------------- data access ------------------
  
  /**
   * Returns the CustomerRecord for the given type and customer, creating it
   * if necessary.
   */
  CustomerRecord getCustomerRecordByPowerType (PowerType type,
                                               CustomerInfo customer)
  {
    HashMap<CustomerInfo, CustomerRecord> customerMap =
        customerProfiles.get(type);
    if (customerMap == null) {
      customerMap = new HashMap<CustomerInfo, CustomerRecord>();
      customerProfiles.put(type, customerMap);
    }
    CustomerRecord record = customerMap.get(customer);
    if (record == null) {
      record = new CustomerRecord(customer);
      customerMap.put(customer, record);
    }
    return record;
  }
  
  /**
   * Returns the customer record for the given tariff spec and customer,
   * creating it if necessary. 
   */
  CustomerRecord getCustomerRecordByTariff (TariffSpecification spec,
                                            CustomerInfo customer)
  {
    HashMap<CustomerInfo, CustomerRecord> customerMap =
        customerSubscriptions.get(spec);
    if (customerMap == null) {
      customerMap = new HashMap<CustomerInfo, CustomerRecord>();
      customerSubscriptions.put(spec, customerMap);
    }
    CustomerRecord record = customerMap.get(customer);
    if (record == null) {
      // seed with the generic record for this customer
      record =
          new CustomerRecord(getCustomerRecordByPowerType(spec.getPowerType(),
                                                          customer));
      customerMap.put(customer, record);
    }
    return record;
  }
  
  /**
   * Finds the list of competing tariffs for the given PowerType.
   */
  List<TariffSpecification> getCompetingTariffs (PowerType powerType)
  {
    List<TariffSpecification> result = competingTariffs.get(powerType);
    if (result == null) {
      result = new ArrayList<TariffSpecification>();
      competingTariffs.put(powerType, result);
    }
    return result;
  }
  //get our tariff from the hashmap
  List<TariffSpecification> getOwnTariffs (PowerType powerType)
  {
    List<TariffSpecification> result = ourTariffs.get(powerType);
    if (result == null) {
      result = new ArrayList<TariffSpecification>();
      ourTariffs.put(powerType, result);
    }
    return result;
  }

  /**
   * Adds a new competing tariff to the list.
   */
  private void addCompetingTariff (TariffSpecification spec)
  {
    getCompetingTariffs(spec.getPowerType()).add(spec);
  } 
  //Add our tariff to our list
  private void addOwnTariff (TariffSpecification spec)
  {
	    getOwnTariffs(spec.getPowerType()).add(spec);
  }

  /**
   * Returns total usage for a given timeslot (represented as a simple index).
   */
  @Override
  public double collectUsage (int index)
  {
    double result = 0.0;
    for (HashMap<CustomerInfo, CustomerRecord> customerMap : customerSubscriptions.values()) {
      for (CustomerRecord record : customerMap.values()) {
        result += record.getUsage(index);
      }
    }
    return -result; // convert to needed energy account balance
  }

  public int collectSubscribers (PowerType pt)
  {
	    int result = 0;
	    for (HashMap<CustomerInfo, CustomerRecord> customerMap : customerSubscriptions.values()) {
	      for (CustomerRecord record : customerMap.values()) {
	    	  if (record.customer == null){ /*System.out.println("Null Customer Record"); */ continue; } //TODO: many null customer records.. Must check
	    	  if (pt == null || record.customer.getPowerType().equals(pt))
	    		  result += record.subscribedPopulation; //Sum up all the subscribers
	      }
	    }
	    return result;
}
  
  // Customer produces or consumes power. We assume the kwh value is negative
  // for production, positive for consumption
  public double getEConsumed(int index)
  {
	    double result = 0.0;
	    for (HashMap<CustomerInfo, CustomerRecord> customerMap : customerSubscriptions.values()) {
	      for (CustomerRecord record : customerMap.values()) {
	    	  if(record.getUsage(index) > 0)
	    		  result += record.getUsage(index); //Sum up all the consumption (Positive)
	      }
	    }
	    return result; 
  }
  public double getTotalStorage(int index)
  {
	  double result = 0.0;
	  for (HashMap<CustomerInfo, CustomerRecord> customerMap : customerSubscriptions.values()) {
	  	for (CustomerRecord record : customerMap.values()) {
	    	//result += record.getCustomerInfo().getStorageCapacity();
	    }
	  }
	  return result;
  }
  public double getEProduced(int index)
  {
	    double result = 0.0;
	    for (HashMap<CustomerInfo, CustomerRecord> customerMap : customerSubscriptions.values()) {
	      for (CustomerRecord record : customerMap.values()) {
	    	  if(record.getUsage(index) < 0)
	    		  result += record.getUsage(index); //Sum up all the production (Negative)
	      }
	    }
	    return result; 
}
public int getTotalCustomers(PowerType pt)
{
	int result = 0;
	for ( CustomerInfo cons : customerRepo.list()){
		if(cons.getPowerType()==pt || pt == null)
			result += cons.getPopulation();
	}
	return result;
}
  // -------------- Message handlers -------------------
  /**
   * Handles CustomerBootstrapData by populating the customer model 
   * corresponding to the given customer and power type. This gives the
   * broker a running start.
   */
  public void handleMessage (CustomerBootstrapData cbd)
  {
    CustomerInfo customer =
            customerRepo.findByNameAndPowerType(cbd.getCustomerName(),
                                                cbd.getPowerType());
    CustomerRecord record = getCustomerRecordByPowerType(cbd.getPowerType(), customer);
    int offset = (timeslotRepo.currentTimeslot().getSerialNumber()
                  - cbd.getNetUsage().length);
    int subs = record.subscribedPopulation;
    record.subscribedPopulation = customer.getPopulation();
    for (int i = 0; i < cbd.getNetUsage().length; i++) {
      record.produceConsume(cbd.getNetUsage()[i], i + offset);
    }
    record.subscribedPopulation = subs;
  }

  /**
   * Handles a TariffSpecification. These are sent by the server when new tariffs are
   * published. If it's not ours, then it's a competitor's tariff. We keep track of 
   * competing tariffs locally, and we also store them in the tariffRepo.
   */
  public void handleMessage (TariffSpecification spec)
  {
      printTariff(spec);
    Broker theBroker = spec.getBroker();
    if (brokerContext.getBrokerUsername().equals(theBroker.getUsername())) {
      if (theBroker != brokerContext)
        // strange bug, seems harmless for now
        log.info("Resolution failed for broker " + theBroker.getUsername());
      // if it's ours, just log it, because we already put it in the repo
      TariffSpecification original =
              tariffRepo.findSpecificationById(spec.getId());
      if (null == original)
        log.error("Spec " + spec.getId() + " not in local repo");
      log.info("published " + spec);
    }
    else {
      // otherwise, keep track of competing tariffs, and record in the repo
      addCompetingTariff(spec);
      tariffRepo.addSpecification(spec);
      
      //Whenever a new Tariff Specification is published we print it.

    }
  }
  
  /**
   * Handles a TariffStatus message. This should do something when the status
   * is not SUCCESS.
   */
  public void handleMessage (TariffStatus ts)
  {
    log.info("TariffStatus: " + ts.getStatus());
  }
  
  /**
   * Handles a TariffTransaction. We only care about certain types: PRODUCE,
   * CONSUME, SIGNUP, and WITHDRAW.
   */
  public void handleMessage(TariffTransaction ttx)
  {
	  //printTx(ttx); //printing the transaction
    // make sure we have this tariff
    TariffSpecification newSpec = ttx.getTariffSpec();
    if (newSpec == null) {
      log.error("TariffTransaction type=" + ttx.getTxType()
                + " for unknown spec");
    }
    else {
      TariffSpecification oldSpec =
              tariffRepo.findSpecificationById(newSpec.getId());
      if (oldSpec != newSpec) {
        log.error("Incoming spec " + newSpec.getId() + " not matched in repo");
      }
    }
    TariffTransaction.Type txType = ttx.getTxType();
    CustomerRecord record = getCustomerRecordByTariff(ttx.getTariffSpec(),
                                                      ttx.getCustomerInfo());
    if (TariffTransaction.Type.SIGNUP == txType) {
      // keep track of customer counts
      record.signup(ttx.getCustomerCount());
    }
    else if (TariffTransaction.Type.WITHDRAW == txType) {
      // customers presumably found a better deal
      record.withdraw(ttx.getCustomerCount());
    }
    else if (TariffTransaction.Type.PRODUCE == txType) {
      // if ttx count and subscribe population don't match, it will be hard
      // to estimate per-individual production
      if (ttx.getCustomerCount() != record.subscribedPopulation) {
        log.warn("production by subset " + ttx.getCustomerCount() +
                 " of subscribed population " + record.subscribedPopulation);
      }
      record.produceConsume(ttx.getKWh(), ttx.getPostedTime());
    }
    else if (TariffTransaction.Type.CONSUME == txType) {
      if (ttx.getCustomerCount() != record.subscribedPopulation) {
        log.warn("consumption by subset " + ttx.getCustomerCount() +
                 " of subscribed population " + record.subscribedPopulation);
      }
      record.produceConsume(ttx.getKWh(), ttx.getPostedTime());      
    }
  }

  /**
   * Handles a TariffRevoke message from the server, indicating that some
   * tariff has been revoked.
   */
  public void handleMessage (TariffRevoke tr)
  {
      System.out.println("Revoking the following Tariff: "); //printing the tariff to be revoked
	  printTariff(tariffRepo.findSpecificationById(tr.getTariffId()));
	  
    Broker source = tr.getBroker();
    log.info("Revoke tariff " + tr.getTariffId()
             + " from " + tr.getBroker().getUsername());
    // if it's from some other broker, we need to remove it from the
    // tariffRepo, and from the competingTariffs list
    if (!(source.getUsername().equals(brokerContext.getBrokerUsername()))) {
      log.info("clear out competing tariff");
      TariffSpecification original =
              tariffRepo.findSpecificationById(tr.getTariffId());
      

	  
      if (null == original) {
        log.warn("Original tariff " + tr.getTariffId() + " not found");
        return;
      }
      tariffRepo.removeSpecification(original.getId());
      List<TariffSpecification> candidates =
              competingTariffs.get(original.getPowerType());
      if (null == candidates) {
        log.warn("Candidate list is null");
        return;
      }
      candidates.remove(original);
    }
  }

  /**
   * Handles a BalancingControlEvent, sent when a BalancingOrder is
   * exercised by the DU.
   */
  public void handleMassage (BalancingControlEvent bce)
  {
    log.info("BalancingControlEvent " + bce.getKwh());
  }

  // --------------- activation -----------------
  /**
   * Called after TimeslotComplete msg received. Note that activation order
   * among modules is non-deterministic.
   */
  @Override // from Activatable
  public void activate (int timeslotIndex)
  {
	//Beginning of timeslot
	printTimeSlot(); //we print the timeslot #
	System.out.println("Current subscribers: " + collectSubscribers(null) + " out of " + getTotalCustomers(null));
	System.out.println("Current production: " + getEProduced(timeslotIndex) + "kwh.");
	System.out.println("Current consumption: " + getEConsumed(timeslotIndex)+ "kwh.");
    if (customerSubscriptions.size() == 0) { //Needs fixing
      // we (most likely) have no tariffs
    	System.out.println("Creating Initial Tarrifs...");
      createInitialTariffs();
    }
    else {
      // we have some, are they good enough?
      improveTariffs();
    }
  }
  
  
  
  
  private double evaluateTariff(CustomerRecord c, TariffSpecification spec){ //Simulated Cost for a week for customer //TODO:Might Need improvement!

	  TariffEvaluationHelper help = new TariffEvaluationHelper();
	  Tariff tf= new Tariff(spec);
	  tf.init();
	  
	  double result = help.estimateCost(tf, c.usage.clone());
	  return Math.abs(result);
	  
  }
  
  private double evaluateTariff(TariffSpecification spec){ //Simulated Cost for a week for customer //TODO:Might Need improvement!
	  //We usually want to minimize evaluateTariff() when we want to improve a tariff. For this reason:
	  //Production will be negative and consumption positive
	  double result = 0.0;
	  PowerType pt = spec.getPowerType();
	  int n = 0;
	  for (CustomerRecord c : customerProfiles.get(pt).values()){
		  if (c.customer == null)
		  	continue;
		  result += evaluateTariff(c, spec); n++;
	  }
	  if(spec.getPowerType().isProduction())
		  result = -result;

	  System.out.println("Tariff " + spec.getId() + " evaluated at: " + result/n);
	  return result/n;
	  
  }
  
  private double normalizedCostDifference(TariffSpecification defaultSpec, TariffSpecification iSpec){
	  double result= 0.0;
	 // int simulationWeeksLeft = brokerContext.
	  //Cost(default) = Sum0->d (Usage[], pv(default), pp(default)...
	  //double CostDefault = evaluateTariff(defaultSpec) * simulationWeeksLeft;
	  //double Costi = evaluateTariff(iSpec)*simulationWeeksLeft + iSpec.getSignupPayment() + defaultSpec.getEarlyWithdrawPayment() + iSpec.getEarlyWithdrawPayment()*Math.min(1.0, iSpec.getMinDuration()/SimulationWeeksLeft);
	  return result;
	
  }
  
  
  // Creates initial tariffs for the main power types. These are simple
  // fixed-rate two-part tariffs that give the broker a fixed margin.
  private void createInitialTariffs ()
  {
    // remember that market prices are per mwh, but tariffs are by kwh
	
    double marketPrice = marketManager.getMeanMarketPrice() / 1000.0;
    // for each power type representing a customer population,
    // create a tariff that's better than what's available
    for (PowerType pt : customerProfiles.keySet()) {
    
      // we'll just do fixed-rate tariffs for now
      double rateValue;
      if (pt.isConsumption())
        rateValue = ((marketPrice + fixedPerKwh) * (1.0 + defaultMargin));
      else
        rateValue = (-1 * marketPrice / (1.0 + defaultMargin));
        //rateValue = -2.0 * marketPrice;
      if (pt.isInterruptible())
        rateValue *= 0.7; // Magic number!! price break for interruptible
      TariffSpecification spec =
          new TariffSpecification(brokerContext.getBroker(), pt)
              .withPeriodicPayment(defaultPeriodicPayment);
      spec.withSignupPayment(defaultSubscriptionPayment);
      spec.withEarlyWithdrawPayment(defaultEarlyWithdraw);
      spec.withMinDuration(defaultMinDuration);
      Rate rate = new Rate().withValue(rateValue);
      if (pt.isInterruptible()) {
        // set max curtailment
        rate.withMaxCurtailment(0.1);
      }
      spec.addRate(rate);
      
      TariffSpecification newspec = surpass(spec);
      customerSubscriptions.put(newspec, new HashMap<CustomerInfo, CustomerRecord>());
      publish(newspec);
    }
  }
  
  private void publish(TariffSpecification newspec)
  {
	  if(newspec == null) { System.out.println("Trying to publish null tariff");return; }
	  tariffRepo.addSpecification(newspec);
	  addOwnTariff(newspec);
      brokerContext.sendMessage(newspec);
  }
  private void revoke(TariffSpecification oldspec)
  {
	  if(oldspec == null) { System.out.println("Trying to revoke null tariff");return; }
	  getOwnTariffs(oldspec.getPowerType()).remove(oldspec);
      TariffRevoke revoke = new TariffRevoke(brokerContext.getBroker(), oldspec);
      brokerContext.sendMessage(revoke);
  }
  
  private void supersede(TariffSpecification oldspec, TariffSpecification newspec)
  {
	  if (newspec == oldspec) { return; } //probably improve() returned the initial tariff so we don't need to publish
	  if (newspec == null) { System.out.println("Trying to supersede with null tariff"); return; } //nothing to do
	  if (oldspec == null){ publish(newspec); return; } //only publish
	  
      newspec.addSupersedes(oldspec.getId()); //So we supersede the old tariff
      publish(newspec); //publish the new one
      revoke(oldspec); // and then revoke the old one

  }
  
  private TariffSpecification surpass(TariffSpecification spec) //we will supersed spec with a tariff better than every competitor's tariff
  {
	  if (spec == null) { System.out.println("Trying to improve NULL tariff."); return null;}
	  
	  PowerType pt = spec.getPowerType();
	  double own = evaluateTariff(spec), best = own;
	  for (TariffSpecification compSpec : getCompetingTariffs(pt)){
		  best = Math.min(evaluateTariff(compSpec), best);
	  }
	  if(own < best) { return null; } //no need to improve anything

	  TariffSpecification newspec =null;
	  do {
		  //improve rate
		  //We improve tariffs by 10% until ours is the best
		  if(newspec == null)
			  newspec = improve(spec);
		  else
			  newspec = improve(newspec);
		  own = evaluateTariff(newspec);
		  
	  }while (best < own);
	  
	  return newspec;
	  
  }
  
  private TariffSpecification improve(TariffSpecification spec)
  {
	  List<TariffSpecification >competition = competingTariffs.get(spec.getPowerType());
	  if (competition == null)
		  return spec; //no competition //?we might want to worsen()?
	  double eval = evaluateTariff(spec);
	  double best = eval;
	  for (TariffSpecification comp : competition)
	  {
		  best = Math.min(evaluateTariff(comp), best);
		  
	  }
	  if (eval == best)
		  return spec; //no improvement needed//?we might want to worsen()?
	  
	  
	  double rateValue, periodic, subscription, withdraw;
	  long minDur;
	  subscription = spec.getSignupPayment();
	  withdraw = spec.getEarlyWithdrawPayment();
	  minDur = spec.getMinDuration();
	  if(spec.getPowerType().isConsumption()){
	      rateValue = spec.getRates().get(0).getValue() *0.8;
	      periodic = spec.getPeriodicPayment()*0.9;
	  }else{ //is production
	      rateValue = spec.getRates().get(0).getValue() *1.1;
	      periodic = spec.getPeriodicPayment()*0.9;
	  }
	    	  
      
      TariffSpecification newspec =
              new TariffSpecification(brokerContext.getBroker(), spec.getPowerType())
                  .withPeriodicPayment(periodic);
      newspec.withSignupPayment(subscription);
      newspec.withEarlyWithdrawPayment(withdraw);
      newspec.withMinDuration(minDur);
      Rate rate = new Rate().withValue(rateValue);
      newspec.addRate(rate);
      
      return newspec;

  }
private TariffSpecification worsen(TariffSpecification spec)
{

	  double rateValue, periodic, subscription, withdraw;
	  long minDur;
	  subscription = spec.getSignupPayment();
	  withdraw = spec.getEarlyWithdrawPayment();
	  minDur = spec.getMinDuration();
	  if(spec.getPowerType().isConsumption()){
	      rateValue = spec.getRates().get(0).getValue() *1.1;
	      periodic = spec.getPeriodicPayment()*1.1;
	      }
	  else{ //is production
	      rateValue = spec.getRates().get(0).getValue() *0.9;
	      periodic = spec.getPeriodicPayment()*1.1;
	  }
    TariffSpecification newspec =
            new TariffSpecification(brokerContext.getBroker(), spec.getPowerType())
                .withPeriodicPayment(periodic);
    newspec.withSignupPayment(subscription);
    newspec.withEarlyWithdrawPayment(withdraw);
    newspec.withMinDuration(minDur);
    Rate rate = new Rate().withValue(rateValue);
    newspec.addRate(rate);
    
    return newspec;
	
	
}
  // Checks to see whether our tariffs need fine-tuning
  private void improveTariffs()
  {
	
    // quick magic-number hack to inject a balancing order
    int timeslotIndex = timeslotRepo.currentTimeslot().getSerialNumber();
    
    
    
    
    
    if(timeslotIndex % 6 == 0)  //Timeslot before tariff publication
    {

    	//First check energy imbalance
    	//Compute Estimated Energy Consumed for the next 6 timeslots so that we change our tariffs in 
    	int i, consumePred=0, producePred=0;
    	for (i = 0; i <6; i ++)
    	{
    		consumePred += getEConsumed(timeslotIndex+i+1);
    		producePred += getEProduced(timeslotIndex+i+1); //negative
    	}
    	double predImbaPrcge = (consumePred+producePred)/(consumePred-producePred);
    	
    	if(predImbaPrcge >0.6)
    		;//Give balancing order with >0 expectage
    	else if (predImbaPrcge < -0.6)
    		;//Give balancing order with <0 expectage ?

   		for (PowerType pt : customerProfiles.keySet()){
   			if(pt.isConsumption()) //we only care for production at this stage
   				continue; 
   			for(TariffSpecification spec: getOwnTariffs(pt)){

       				//Improve The production Tariff
       				if(predImbaPrcge >0.1){
       					System.out.println("(E)Trying to improve:");
           				printTariff(spec);
           				TariffSpecification newspec = improve(spec);
           				supersede(spec, newspec);
       				}else{
       					System.out.println("(E)Trying to worsen:");
           				printTariff(spec);
           				TariffSpecification newspec = worsen(spec);
           				supersede(spec, newspec);
       				}

    		}

    	}
   		
    	//now check our subscribed customers
    	double customerPercentage = 0.0;
   		for (PowerType pt : customerProfiles.keySet()){
   			if(pt.isProduction()) //we only care for consumption at this stage
   				continue; 
   			customerPercentage = ((double)collectSubscribers(pt))/getTotalCustomers(pt);
   			for(TariffSpecification spec: getOwnTariffs(pt)){
       				//Improve The consumption Tariff to get more customers
       				if(customerPercentage < 0.33){
       					System.out.println("(C)Trying to surpass:");
           				printTariff(spec);
           				//TariffSpecification newspec = improve(spec);
           				TariffSpecification newspec = surpass(spec);
           				supersede(spec, newspec);
       				}else if (customerPercentage < 0.70){
       					System.out.println("(C)Trying to improve:");
       					printTariff(spec);
       					TariffSpecification newspec = improve(spec);
       					supersede(spec, newspec);
       					//Let's not worsen our tariffs at this stage..
       					//System.out.println("Trying to worsen:");
           				//printTariff(spec);
           				//TariffSpecification newspec = worsen(spec);
           				//supersede(spec, newspec);
       				}
    		}

    	}
    }
    
    
    
    
    
    /*
    if (371 == timeslotIndex) { //In time slot 371 the guy here asks for a balancing order.
      for (TariffSpecification spec :
           tariffRepo.findTariffSpecificationsByBroker(brokerContext.getBroker())) {
    	
        if (PowerType.INTERRUPTIBLE_CONSUMPTION == spec.getPowerType()) { //Yoda Condition? :p
        
          BalancingOrder order = new BalancingOrder(brokerContext.getBroker(),
                                                    spec, 
                                                    0.5,
                                                    spec.getRates().get(0).getMinValue() * 0.9);
          
          brokerContext.sendMessage(order); //Sending Balancing order.. TODO: CHECK
        }
      }
    }
    // magic-number hack to supersede a tariff
    if (380 == timeslotIndex) {
      // find the existing CONSUMPTION tariff
      TariffSpecification oldc = null;
      List<TariffSpecification> candidates =
              tariffRepo.findTariffSpecificationsByPowerType(PowerType.CONSUMPTION);
      if (null == candidates || 0 == candidates.size())
        log.error("No CONSUMPTION tariffs found");
      else {
        oldc = candidates.get(0);
      }

      double rateValue = oldc.getRates().get(0).getValue();
      // create a new CONSUMPTION tariff
      TariffSpecification spec =
              new TariffSpecification(brokerContext.getBroker(), PowerType.CONSUMPTION)
                  .withPeriodicPayment(defaultPeriodicPayment * 1.1);
      Rate rate = new Rate().withValue(rateValue);
      spec.addRate(rate);
      //The superseding tariff must be received
      //(but not necessarily published) before revoking the original tariff. All subscriptions to the original
     // tariff will be moved to the superseding tariff during the next tariff-publication cycle.
      if (null != oldc)
        spec.addSupersedes(oldc.getId()); //So we supersede the old tariff
      tariffRepo.addSpecification(spec);
      brokerContext.sendMessage(spec);
      // and then revoke the old one
      TariffRevoke revoke = new TariffRevoke(brokerContext.getBroker(), oldc);
      brokerContext.sendMessage(revoke);
    }
    */
  }

  // ------------- test-support methods ----------------
  double getUsageForCustomer (CustomerInfo customer,
                              TariffSpecification tariffSpec,
                              int index)
  {
    CustomerRecord record = getCustomerRecordByTariff(tariffSpec, customer);
    return record.getUsage(index);
  }
  
  // test-support method
  HashMap<PowerType, double[]> getRawUsageForCustomer (CustomerInfo customer)
  {
    HashMap<PowerType, double[]> result = new HashMap<PowerType, double[]>();
    for (PowerType type : customerProfiles.keySet()) {
      CustomerRecord record = customerProfiles.get(type).get(customer);
      if (record != null) {
        result.put(type, record.usage);
      }
    }
    return result;
  }

  // test-support method
  HashMap<String, Integer> getCustomerCounts()
  {
    HashMap<String, Integer> result = new HashMap<String, Integer>();
    for (TariffSpecification spec : customerSubscriptions.keySet()) {
      HashMap<CustomerInfo, CustomerRecord> customerMap = customerSubscriptions.get(spec);
      for (CustomerRecord record : customerMap.values()) {
        result.put(record.customer.getName() + spec.getPowerType(), 
                    record.subscribedPopulation);
      }
    }
    return result;
  }

  //-------------------- Customer-model recording ---------------------
  /**
   * Keeps track of customer status and usage. Usage is stored
   * per-customer-unit, but reported as the product of the per-customer
   * quantity and the subscribed population. This allows the broker to use
   * historical usage data as the subscribed population shifts.
   */
  class CustomerRecord
  {
    CustomerInfo customer;
    int subscribedPopulation = 0;
    double[] usage;
//TODO:make a Hashmap in the form of <Customer, ArrayList[][]>
    List<Double> consumptionHistory; 
    double alpha = 0.3;
    
    
    /**
     * Creates an empty record
     */
    CustomerRecord (CustomerInfo customer)
    {
      super();
      this.customer = customer;
      this.usage = new double[brokerContext.getUsageRecordLength()];
      consumptionHistory = new ArrayList<Double>();
      
    //  this.customerHistory=new ArrayList[24][7];
      
    //  for (ArrayList[] i : customerHistory){
//	for (ArrayList cell : i){
//	  cell=new ArrayList<CustomerHistoryDetails>();
//	}
  //    }
    }
    
    CustomerRecord (CustomerRecord oldRecord)
    {
      super();
      this.customer = oldRecord.customer;
      this.usage = Arrays.copyOf(oldRecord.usage, brokerContext.getUsageRecordLength());
      consumptionHistory = new ArrayList<Double>();
  //    this.customerHistory=new ArrayList[24][7];
      
    //  for (ArrayList[] i : customerHistory){
//	for (ArrayList cell : i){
//	  cell=new ArrayList<CustomerHistoryDetails>();
//	}
  //    }
 
    }
    
    double getConsumption(int day, int hour)
    {
    	return consumptionHistory.get(day*24+hour);
    }
    // Returns the CustomerInfo for this record
    CustomerInfo getCustomerInfo ()
    {
      return customer;
    }
    
    // Adds new individuals to the count
    void signup (int population)
    {
      subscribedPopulation = Math.min(customer.getPopulation(),
                                      subscribedPopulation + population);
    }
    
    // Removes individuals from the count
    void withdraw (int population)
    {
      subscribedPopulation -= population;
    }
    
    // Customer produces or consumes power. We assume the kwh value is negative
    // for production, positive for consumption
    void produceConsume (double kwh, Instant when)
    {
      int index = getIndex(when);
      produceConsume(kwh, index);
    }
    
    // store profile data at the given index
    void produceConsume (double kwh, int rawIndex)
    {
      int index = getIndex(rawIndex);
      double kwhPerCustomer = kwh / (double)subscribedPopulation;
      
      
      while(consumptionHistory.size() < rawIndex-1)
    	  consumptionHistory.add(0.0);
      consumptionHistory.add(kwhPerCustomer);
      
      double oldUsage = usage[index];
      if (oldUsage == 0.0) {
        // assume this is the first time
        usage[index] = kwhPerCustomer;
      }
      else {
        // exponential smoothing
        usage[index] = alpha * kwhPerCustomer + (1.0 - alpha) * oldUsage;
      }
      log.debug("consume " + kwh + " at " + index +
                ", customer " + customer.getName());
    }
    
    double getUsage (int index)
    {
      if (index < 0) {
        log.warn("usage requested for negative index " + index);
        index = 0;
      }
      //System.out.println("Usage: " + usage[getIndex(index)] + "Subs: " + subscribedPopulation);
      //System.out.println(" Usage: " + usage[getIndex(index)] + "subscribed population: " + subscribedPopulation);
      return (usage[getIndex(index)] * (double)subscribedPopulation);
    }
    
    // we assume here that time slot index always matches the number of
    // time slots that have passed since the beginning of the simulation.
    int getIndex (Instant when)
    {
      int result = (int)((when.getMillis() - timeService.getBase()) /
                         (Competition.currentCompetition().getTimeslotDuration()));
      return result;
    }
    
    private int getIndex (int rawIndex)
    {
      return rawIndex % usage.length;
    }

    /*
    *function to populate customerHistory
    */
    void populateCustomerHistory(int rawIndex, double kwh, WeatherReport weather)
    {
    	
    	//CustomerHistArray[rawIndex%24][rawIndex/24]=new CustomerHistoryDetails(kwh, weather);
    	//CustomerHistoryDetails [rawIndex%24][rawIndex/24].add(new CustomerHistoryDetails(kwh, weather));
    }  
}


  //TODO:maybe weather is no needed!! (If so, this class is useless. In that case just replace this class with kwh)
  /**
  *CustomerHistoryDetails class
  *per customer model per timeslot
  * detailed consumption/production history
  *and weather report.
  *
  *used for prediction
  */
  class CustomerHistoryDetails
  {
	double kwh;
	WeatherReport weather;
	
        CustomerHistoryDetails(double kwh,WeatherReport weather)
	{
		this.kwh=kwh;
		this.weather=weather;
	}
    
  }
}

