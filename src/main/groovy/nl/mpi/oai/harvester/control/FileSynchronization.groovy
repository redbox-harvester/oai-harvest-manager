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

package nl.mpi.oai.harvester.control

import ORG.oclc.oai.harvester2.verb.ListIdentifiers
import nl.mpi.oai.harvester.Provider
import nl.mpi.oai.harvester.utils.Statistic
import org.apache.commons.io.FileUtils
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.w3c.dom.NodeList

import javax.xml.xpath.XPathConstants
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Stream

import static nl.mpi.oai.harvester.Provider.DeletionMode.*

/**
 *   Utility class used in incremental harvest process for synchronizing files
 */
final class FileSynchronization {

    private static final Logger logger = LogManager.getLogger(FileSynchronization.class)

    private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd")
    private static final String currentDate = formatter.format(new Date())
    private static final String CMDI = "/other/cmdi/"
    private static final String CMDI1_1 = "/other/cmdi-1_1/"
    private static final String CMDI1_2 = "/other/cmdi-1_2/"

    private static final ConcurrentHashMap<Provider, Statistic> statistic = new ConcurrentHashMap<>()

    static void execute(Provider provider) {

        switch (provider.getDeletionMode()) {

            case NO:
                runSynchronizationForNoDeletionMode(provider)
                break
            case TRANSIENT:
            case PERSISTENT:
                runSynchronizationForTransientDeletionMode(provider)
                break
            default:
                break
        }
        saveStatistics(provider)
    }

    private static void runSynchronizationForTransientDeletionMode(final Provider provider) {
        String dir = Main.config.getWorkingDirectory() + CMDI
        File file = new File(dir + Util.toFileFormat(provider.getName()) + "_remove.txt")

        String firstDirToRemove = Main.config.getWorkingDirectory() + CMDI + Util.toFileFormat(provider.getName()) + "/"
        String scenedDirToRemove = Main.config.getWorkingDirectory() + CMDI1_1 + Util.toFileFormat(provider.getName()) + "/"
        String thirdDirToRemove = Main.config.getWorkingDirectory() + CMDI1_2 + Util.toFileFormat(provider.getName()) + "/"

        delete(provider, file, firstDirToRemove)
        delete(provider, file, scenedDirToRemove)
        delete(provider, file, thirdDirToRemove)
        FileUtils.deleteQuietly(file)
    }

    private static void runSynchronizationForNoDeletionMode(final Provider provider) {
        String dir1 = Main.config.getWorkingDirectory() + CMDI + Util.toFileFormat(provider.getName())
        String dir2 = Main.config.getWorkingDirectory() + CMDI1_2 + Util.toFileFormat(provider.getName())
        String dir3 = Main.config.getWorkingDirectory() + CMDI1_1 + Util.toFileFormat(provider.getName())

        File file = new File(dir1 + "/current.txt")
        String resumptionToken = null
        boolean done = false


        new FileWriter(file, true).withWriter { writer ->
            int counter = 0
            while (!done) {
                if (counter == provider.maxRetryCount) {
                    break
                } else {
                    int retryDelay = provider.getRetryDelay(counter)
                    if (retryDelay > 0) {
                        try {
                            Thread.sleep(retryDelay)
                        } catch (InterruptedException e) {
                            logger.error(e.getMessage(), e)
                        }
                    }
                }
                try {
                    def listIdentifiers
                    if (!(resumptionToken == null || resumptionToken.isEmpty())) {
                        listIdentifiers = new ListIdentifiers(provider.oaiUrl, resumptionToken)
                    } else {
                        listIdentifiers = new ListIdentifiers(provider.oaiUrl, null, null, null, "cmdi", 60)
                    }

                    resumptionToken = listIdentifiers.getResumptionToken()

                    if (resumptionToken == null || resumptionToken.isEmpty()) {
                        done = true
                    }
                    NodeList nodeList = (NodeList) provider.xpath.evaluate(
                            "//*[starts-with(local-name(),'identifier') "
                                    + "and parent::*[local-name()='header' "
                                    + "and not(@status='deleted')]]/text()",
                            listIdentifiers.getDocument(), XPathConstants.NODESET)

                    for (int j = 0; j < nodeList.getLength(); j++) {
                        String identifier = nodeList.item(j).getNodeValue()
                        writer.write(Util.toFileFormat(identifier) + ".xml\n")
                    }
                } catch (Exception ex) {
                    counter++
                    logger.error("Error while running ListIdentifiers synchronization " + file + ": ", ex)
                    done = false
                }
            }
        }
        move(file, dir1)
        move(file, dir2)
        move(file, dir3)
        deleteDirectory(dir1, provider)
        deleteDirectory(dir2, provider)
        deleteDirectory(dir3, provider)

        FileUtils.deleteQuietly(file)
    }

    /**
     *   Removes temporary directory and renames to original name
     */
    private static void deleteDirectory(final String dir, final Provider provider) {
        File[] files = new File(dir).listFiles()
        if (files != null) {
            for (File f : files) {
                if (f.isFile()) {
                    Path path = Paths.get(dir + "/" + f.getName())
                    try {
                        Files.delete(path)
                        saveToHistoryFile(provider, path, Operation.DELETE)
                    } catch (IOException e) {
                        logger.error("Unable to delete File " + path + ": ", e)
                    }
                }
            }
        }
        Path directory = Paths.get(dir)
        Files.delete(directory)
        File toRename = new File(dir + "_new")
        toRename.renameTo(new File(dir))
    }

    private static Stream<String> getAsStream(final File file) {
        Files.lines(Paths.get(file.toURI()))
    }

    /**
     *   Move file  of temporary directory
     */
    private static void move(final File file, final String dir) {
        Stream<String> fileStream = getAsStream(file)

        if (fileStream != null) {
            fileStream.each { String l ->
                FileUtils.moveFileToDirectory(
                        FileUtils.getFile(dir + "/" + l),
                        FileUtils.getFile(dir + "_new/"), true)
            }
        }
    }

    /**
     *
     *   Removes files based on list provided in file
     */
    private static void delete(final Provider provider, final File file, final String dir) {
        Stream<String> fileStream = getAsStream(file)

        if (fileStream != null) {
            fileStream.each { l ->
                Path path = Paths.get(dir + l)
                if (Files.exists(path)) {
                    Files.delete(path)
                    saveToHistoryFile(provider, path, Operation.DELETE)
                }
            }
        }
    }

    static void saveStatistics(final Provider provider) {
        String dir = Main.config.getWorkingDirectory() + CMDI
        FileUtils.forceMkdir(new File(dir))
        File file = new File(dir + Util.toFileFormat(provider.getName()) + "_history.xml")
        Statistic stats = statistic.get(provider)
        StringBuffer sb = new StringBuffer()
        sb.append("<harvest date=\"").append(currentDate).append("\" ")
                .append("operationTime=\"" + stats.getHarvestTime() + "s\" ")
                .append("requestsToServer=\"" + stats.getRequests() + "\" ")
                .append("collectedRecords=\"" + stats.getHarvestedRecords() + "\" ")
                .append("/>\n")
        writeToHistoryFile(file, sb.toString())
    }

    private static void writeToHistoryFile(final File file, String toSave) {
        new FileWriter(file, true).withWriter { deltaWriter ->
            deltaWriter.write(toSave)
        }

    }

    static Statistic getProviderStatistic(Provider provider) {
        return statistic.get(provider)
    }

    static void addProviderStatistic(Provider provider) {
        statistic.put(provider, new Statistic())
    }

    static void saveToHistoryFile(final Provider provider, final Path filePath, final Operation operation) {
        String dir = Main.config.getWorkingDirectory() + CMDI
        File file = new File(dir + Util.toFileFormat(provider.getName()) + "_history.xml")
        StringBuffer sb = new StringBuffer()
        sb.append("<file ")
                .append("harvestDate=\"").append(currentDate).append("\" ")
                .append("name=\"").append(filePath.getFileName()).append("\" ")
                .append("operation=\"" + operation.name()).append("\" ")
                .append("/>\n")
        writeToHistoryFile(file, sb.toString())
    }

    static void saveFilesToRemove(String file, Provider provider) {
        String dir = Main.config.getWorkingDirectory() + CMDI + Util.toFileFormat(provider.getName())
        new FileWriter(new File(dir + "_remove.txt"), true).withWriter { writer ->
            writer.write(file + "\n")
        }
    }

    static enum Operation {
        INSERT, DELETE
    }
}