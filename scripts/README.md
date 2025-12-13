<h1>Testing Scripts</h1>
<p>The scripts are used to more conveniently try out the repository. There are 3 main parts: setting up, running, and aggregating. They're put in a folder accordingly.</p>
<p>Note: The configurations of the files running in this folder is independent from the main repo.</p>

<h2>1. Setting Up</h2>
<p>They set up nfs for the targeted machines as the file name specified. The machine's address can be specified in conf.json. These are similar to start_mount.py</p>
<ul>
    <li>setup_master_nfs.py</li>
    <li>setup_master_nfs.py</li>
</ul>

<h2>2. Running</h2>
<p>Run the repo. Please run nodes first before running clients (and wait until they're waiting for client connections)</p>
<ol>
    <li>run_nodes.py</li>
    <li>run_clients.py</li>
</ol>

<h2>3. Aggregation</h2>
<p>Please modify the experiment number variable and root directory in each files (there's only one of them per file and their variable should be clear enough) as appropriate</p>

- convert_csv.py
    - Converts log files of the alias and combine them as a csv file. Please run this one before running everything else
- aggregate.py
    - Takes average latency for all clients for a given protocol.
- cdf.py
    - Produce a cdf in csv format

<h2>Usage</h2>
<ol>
    <li>Please make sure each machines specified in conf.json has go installed. Afterwards, make sure they have nfs set up by running the setup nfs files.</li>
    <li>Adjust the actual config file for the repo (e.g. aws.conf, local.conf, etc.) and set the experiment number along with protocol in conf.json accordingly.</li>
    <li>Run the nodes</li>
    <li>Run the clients</li>
    <li>Repeat these 3 steps until satisfied</li>
    <li>Aggregate the experiments for a particular experiment number</li>
</ol>