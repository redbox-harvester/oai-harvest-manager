/*
 * Copyright (C) 2014, The Max Planck Institute for
 * Psycholinguistics.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * A copy of the GNU General Public License is included in the file
 * LICENSE-gpl-3.0.txt. If that file is missing, see
 * <http://www.gnu.org/licenses/>.
 */

package nl.mpi.oai.harvester.control;

import nl.mpi.oai.harvester.Provider;
import nl.mpi.oai.harvester.StaticProvider;
import nl.mpi.oai.harvester.action.ActionSequence;
import nl.mpi.oai.harvester.cycle.Cycle;
import nl.mpi.oai.harvester.cycle.CycleProperties;
import nl.mpi.oai.harvester.cycle.Endpoint;
import nl.mpi.oai.harvester.harvesting.*;
import nl.mpi.oai.harvester.metadata.MetadataFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class represents a single processing thread in the harvesting actions
 * workflow. In practice one worker takes care of one provider. The worker
 * applies a scenario for harvesting: first get record identifiers, after that
 * get the records individually. Alternatively, in a second scenario, it gets
 * multiple records per OAI request directly.
 *
 * @author Lari Lampen (MPI-PL), extensions by Kees Jan van de Looij (MPI-PL).
 */
public class Worker implements Runnable {

    private static final Logger logger = LogManager.getLogger(Worker.class);

    /**
     * A standard semaphore is used to track the number of running threads.
     */
    private static Semaphore semaphore;

    private static AtomicInteger total = new AtomicInteger();

    /**
     * The provider this worker deals with.
     */
    private final Provider provider;

    /**
     * List of actionSequences to be applied to the harvested metadata.
     */
    private final List<ActionSequence> actionSequences;

    /* Harvesting scenario to be applied. ListIdentifiers: first, based on
       endpoint data and prefix, get a list of identifiers, and after that
       retrieve each record in the list individually. ListRecords: skip the
       list, retrieve multiple records per request.
     */
    private final CycleProperties.Scenario scenarioName;

    // kj: annotate
    Endpoint endpoint;

    /**
     * Set the maximum number of concurrent worker threads.
     *
     * @param num number of running threads that may not be exceeded
     */
    public static void setConcurrentLimit(int num) {
        semaphore = new Semaphore(num);
    }

    public static int getTotal() {
        return total.get();
    }


    /**
     * Associate a provider and action actionSequences with a scenario
     *
     * @param provider        OAI-PMH provider that this thread will harvest
     * @param actionSequences list of actions to take on harvested metadata
     * @param cycle           the harvesting cycle
     *                        <p>
     *                        kj: ideally, give a worker the endpoint URI instead of the provider
     */
    public Worker(Provider provider, List<ActionSequence> actionSequences,
                  Cycle cycle) {

        this.provider = provider;

        this.actionSequences = actionSequences;

        // register the endpoint with the cycle, kj: get the group
        endpoint = cycle.next(provider.getOaiUrl(), "group", provider.getScenario());

        // get the name of the scenario the worker needs to apply
        this.scenarioName = endpoint.getScenario();
        total.incrementAndGet();
    }

    /**
     * <br>Start this worker thread <br><br>
     * <p>
     * This method will block for as long as necessary until a thread can be
     * started without violating the limit.
     */
    public void startWorker() {
        for (; ; ) {
            try {
                semaphore.acquire();
                break;
            } catch (InterruptedException e) {
            }
        }
        Thread t = new Thread(this);
        t.start();
    }

    @Override
    public void run() {
        Throwable t = null;
        try {
            logger.debug("Welcome to OAI Harvest Manager worker!");
            provider.init();


            // setting specific log filename
            ThreadContext.put("logFileName", Util.toFileFormat(provider.getName()).replaceAll("/", ""));

            boolean done = false;

            // factory for metadata records
            MetadataFactory metadataFactory = new MetadataFactory();

            // factory for OAI verbs
            OAIFactory oaiFactory = new OAIFactory();

            logger.info("Processing provider " + provider + " using " + scenarioName + " scenario and timeout " + provider.getTimeout() + " and retry (" + provider.getMaxRetryCount() + "," + provider.getRetryDelays() + ")");

            FileSynchronization.addProviderStatistic(provider);

            for (final ActionSequence actionSequence : actionSequences) {

                // list of prefixes provided by the endpoint
                List<String> prefixes;

                // kj: annotate
                Scenario scenario = new Scenario(provider, actionSequence);

                if (provider instanceof StaticProvider) {
                    logger.debug("static harvest[" + provider + "]");

                    // set type of format harvesting to apply
                    AbstractHarvesting harvesting = new StaticPrefixHarvesting(
                            oaiFactory,
                            (StaticProvider) provider,
                            actionSequence);
                    logger.debug("harvesting[" + harvesting + "]");

                    // get the prefixes
                    prefixes = scenario.getPrefixes(harvesting);
                    logger.debug("prefixes[" + prefixes + "]");

                    if (prefixes.isEmpty()) {
                        logger.debug("no prefixes[" + prefixes + "] -> done");
                        done = false;
                    } else {
                        // set type of record harvesting to apply
                        harvesting = new StaticRecordListHarvesting(oaiFactory,
                                (StaticProvider) provider, prefixes, metadataFactory);

                        // get the records
                        if (scenarioName == CycleProperties.Scenario.ListIdentifiers) {
                            done = scenario.listIdentifiers(harvesting);
                            logger.debug("list identifiers -> done[" + done + "]");
                        } else {
                            done = scenario.listRecords(harvesting);
                            logger.debug("list records -> done[" + done + "]");
                        }
                    }
                } else {
                    logger.debug("dynamic harvest[" + provider + "]");

                    // set type of format harvesting to apply
                    AbstractHarvesting harvesting = new FormatHarvesting(oaiFactory,
                            provider, actionSequence);

                    // get the prefixes
                    prefixes = scenario.getPrefixes(harvesting);
                    logger.debug("prefixes[" + prefixes + "]");

                    if (prefixes.isEmpty()) {
                        // no match
                        logger.debug("no prefixes[" + prefixes + "] -> done");
                        done = false;
                    } else {
                        // determine the type of record harvesting to apply
                        if (scenarioName == CycleProperties.Scenario.ListIdentifiers) {
                            // kj: annotate, connect verb to scenario
                            harvesting = new IdentifierListHarvesting(oaiFactory,
                                    provider, prefixes, metadataFactory, endpoint);

                            // get the records
                            done = scenario.listIdentifiers(harvesting);
                            logger.debug("list identifiers -> done[" + done + "]");
                        } else {
                            harvesting = new RecordListHarvesting(oaiFactory,
                                    provider, prefixes, metadataFactory, endpoint);

                            // get the records
                            done = scenario.listRecords(harvesting);
                            logger.debug("list records -> done[" + done + "]");
                        }
                        if (Main.config.isIncremental() && endpoint.allowIncrementalHarvest()) {
                            FileSynchronization.execute(provider);
                        }
                    }
                }

                // break after an action sequence has completed successfully
                if (done) break;

            }

            // report back success or failure to the cycle
            endpoint.doneHarvesting(done);
            FileSynchronization.saveStatistics(provider);
            endpoint.setIncrement(FileSynchronization.getProviderStatistic(provider).getHarvestedRecords());
            logger.info("Processing finished for " + provider);
        } catch (Throwable e) {
            logger.error("Processing failed for " + provider + ": " + e.getMessage(), e);
            t = e;
            throw e;
        } finally {
            provider.close();

            ThreadContext.clearAll();

            // tell the main log how it went
            if (t != null)
                logger.error("Processing failed for " + provider + ": " + t.getMessage(), t);
            else
                logger.info("Processing finished for " + provider);

            semaphore.release();
            total.decrementAndGet();
            logger.debug("Goodbye from OAI Harvest Manager worker!");
        }
    }

}


