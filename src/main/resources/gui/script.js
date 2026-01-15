document.addEventListener("DOMContentLoaded", function () {
    const loadingSpinner = document.getElementById("loading");
    const metadataContainer = document.getElementById("metadata-container");

    // Show loading spinner
    loadingSpinner.style.display = "block";

    const projectTitleElement = document.getElementById("project-title");

    // Fetch project ID from the endpoint
    fetch('/internal/projectId')
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.text(); // Assuming the endpoint returns plain text
        })
        .then(projectId => {
            projectTitleElement.textContent = `${projectId}`;
        })
        .catch(error => {
            console.error("Error fetching project ID:", error);
        });

    // Fetch metadata
    fetch('/internal/metadata')
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            loadingSpinner.style.display = "none"; // Hide loading spinner
            renderMetadata(data); // Render metadata
            highlightUnmappedFields();
        })
        .catch(error => {
            loadingSpinner.style.display = "none"; // Hide loading spinner
            metadataContainer.innerHTML = `<p style="color:red;">Failed to load metadata: ${error.message}</p>`;
        });

    function renderMetadata(metadata) {
        metadataContainer.innerHTML = ''; // Clear any existing content

        Object.keys(metadata).forEach(datasetName => {
            // Create dataset section
            const datasetSection = document.createElement("div");
            datasetSection.classList.add("dataset-section");

            // Dataset header
            const datasetHeader = document.createElement("div");
            datasetHeader.classList.add("dataset-header");
            datasetHeader.textContent = datasetName;
            datasetSection.appendChild(datasetHeader);

            // Add tables for this dataset
            const tables = metadata[datasetName].tables;
            tables.forEach(table => {
                // Create table container
                const tableDiv = document.createElement("div");
                tableDiv.classList.add("table");

                // Table header (expandable bar)
                //const tableHeader = document.createElement("div");
                //tableHeader.classList.add("table-header");
                //tableHeader.textContent = `${table.tableName} (${table.numRows} rows)`;
                //tableDiv.appendChild(tableHeader);

                //

                // Table header (expandable bar)
                /*
                const tableHeader = document.createElement("div");
                tableHeader.classList.add("table-header"); // Add styles for the header container
                tableHeader.textContent = table.tableName;

                // Create a separate div for the row count
                const rowCountDiv = document.createElement("div");
                rowCountDiv.classList.add("row-count");
                rowCountDiv.textContent = `${table.numRows} records`;

                // Append the row count div to the header
                tableHeader.appendChild(rowCountDiv);
                 */

                // Table header (expandable bar)
                const tableHeader = document.createElement("div");
                tableHeader.classList.add("table-header"); // Add styles for the header container

                // Wrapper div to group table name and the "inactive" label together
                const nameAndLabelWrapper = document.createElement("div");
                nameAndLabelWrapper.classList.add("name-and-label-wrapper");

                // Table name
                const tableNameDiv = document.createElement("div");
                tableNameDiv.textContent = table.tableName;

                // Create the 'inactive' label div
                const inactiveLabel = document.createElement("div");
                inactiveLabel.textContent = 'INACTIVE';
                inactiveLabel.classList.add('inactive-label');
                inactiveLabel.title = "No records will be transferred to Big Query in daily job or bulk transfer job";

                // Append the table name and inactive label into the wrapper
                nameAndLabelWrapper.appendChild(tableNameDiv);
                if (!table.active) {
                    nameAndLabelWrapper.appendChild(inactiveLabel);
                }

                // Create a separate div for the row count
                const rowCountDiv = document.createElement("div");
                rowCountDiv.classList.add("row-count");
                rowCountDiv.textContent = `${table.numRows} records`;

                // Append the wrapper and row count div to the table header
                tableHeader.appendChild(nameAndLabelWrapper);
                tableHeader.appendChild(rowCountDiv);


                // Add the header to the table div
                tableDiv.appendChild(tableHeader);

                // Container for table details NEW
                const tableDetails = document.createElement("div");
                tableDetails.classList.add("table-details");
                tableDetails.style.display = "none"; // Hidden by default

                // Add Salesforce query NEW
                if (table.salesforceQuery) {
                    const queryDiv = document.createElement("div");
                    queryDiv.classList.add("salesforce-query");
                    queryDiv.textContent = table.salesforceQuery;
                    tableDetails.appendChild(queryDiv);

                    if (table.useForLastModifiedDate !== "LastModifiedDate") {
                        lastModifiedFieldDiv = document.createElement("div");
                        lastModifiedFieldDiv.classList.add("salesforce-query");
                        lastModifiedFieldDiv.classList.add("last-modified-field");
                        lastModifiedFieldDiv.textContent = table.useForLastModifiedDate + " used instead of LastModifiedDate";
                        tableDetails.appendChild(lastModifiedFieldDiv);
                    }

                    // Add buttons row
                    const buttonRow = document.createElement("div");
                    buttonRow.classList.add("button-row");
                    // Test Salesforce Query Button
                    const testQueryButton = document.createElement("button");
                    testQueryButton.classList.add("test-query-button");
                    testQueryButton.textContent = "Test Salesforce Query";

                    // Spinner next to the button
                    const spinner = document.createElement("div");
                    spinner.classList.add("small-spinner");

                    testQueryButton.addEventListener("click", () => {
                        //resultDiv.textContent = ''; // Clear previous results
                        const originalText = testQueryButton.textContent; // Save the original button text
                        testQueryButton.textContent = ''; // Clear the button text
                        spinner.style.display = 'inline-block';
                        testQueryButton.appendChild(spinner); // Add spinner to the button
                        fetch('/internal/testSalesforceQuery?dataset='+ datasetName+ '&table=' + table.tableName, {
                            method: "GET",
                        })
                            .then(response => {
                                if (response.ok) {
                                    // Parse as JSON for successful responses
                                    return response.json().then(data => ({
                                        status: response.status,
                                        body: data
                                    }));
                                } else {
                                    // Handle non-200 responses as plain text
                                    return response.text().then(text => ({
                                        status: response.status,
                                        body: text
                                    }));
                                }
                            })
                            .then(({ status, body }) => {
                                const resultDiv = document.createElement("div");
                                resultDiv.classList.add("query-result");

                                testQueryButton.removeChild(spinner); // Remove spinner
                                testQueryButton.textContent = originalText; // Restore button text

                                if (status === 200) {
                                    const first = body.first === 1000 ? "1000+" : body.first;
                                    const second = body.second === 1000 ? "1000+" : body.second;
                                    resultDiv.textContent = `Found ${first} records yesterday, ${second} records last 5 days.`;
                                    resultDiv.classList.add(
                                        body.first === 0 && body.second === 0 ? "result-blue" : "result-green"
                                    );
                                } else {
                                    resultDiv.innerHTML = `${body}`;
                                    resultDiv.classList.add("result-red");
                                }

                                tableColumns.insertBefore(resultDiv, tableColumnsTable);
                            })
                            .catch(err => {
                                spinner.style.display = "none";
                                const resultDiv = document.createElement("div");
                                resultDiv.textContent = `${err.message}`;
                                resultDiv.classList.add("query-result", "result-red");
                                tableColumns.insertBefore(resultDiv, tableColumnsTable);
                            });
                    });

                    const prepareBulkButton = document.createElement("button");
                    prepareBulkButton.classList.add("bulk-transfer-button");
                    prepareBulkButton.textContent = "Prepare full bulk transfer";

                    prepareBulkButton.addEventListener('click', () => {
                        if (table.numRows > 0) {
                            // Check if the modal is already visible
                            const modal = document.getElementById('confirmation-modal');
                            const message = document.getElementById('confirmation-message');
                            let confirmYes = document.getElementById('confirm-yes');
                            let confirmNo = document.getElementById('confirm-no');

                            if (!modal.classList.contains('hidden')) {
                                // Modal is already visible, ignore subsequent clicks
                                return;
                            }

                            // Show the modal
                            message.innerHTML = 'Are you sure?<br>This table contains records. It is recommended to clear the table in BigQuery before performing a full bulk transfer.';
                            modal.classList.remove('hidden');

                            // Add one-time event listeners for the modal buttons
                            const proceedWithBulkTransfer = async () => {
                                modal.classList.add('hidden');
                                try {
                                    await reset(); // Wait for the reset call to complete
                                    console.log("Reset completed. Proceeding with bulk transfer...");
                                    performBulkTransfer(); // Call the shared logic
                                } catch (error) {
                                    console.error("Error during reset:", error);
                                    // Handle the error if necessary
                                }
                            };

                            const cancelBulkTransfer = () => {
                                modal.classList.add('hidden');
                            };

                            // Clear all event listeners for the confirm buttons
                            confirmYes = clearEventListeners(confirmYes);
                            confirmNo = clearEventListeners(confirmNo);

                            confirmYes.addEventListener('click', proceedWithBulkTransfer);
                            confirmNo.addEventListener('click', cancelBulkTransfer);

                            return; // Prevent continuing to the bulk transfer until confirmed
                        } else {
                            performBulkTransfer(); // Call the shared logic if no confirmation is needed
                        }


                    });

                    function reset() {
                        return fetch(`/internal/reset?dataset=${datasetName}&table=${table.tableName}`, {
                            method: "GET" // GET request for this endpoint
                        }).then(r => {
                            console.log("reset:", r.status);
                            return r; // Ensure the promise resolves with the fetch response
                        });
                    }

                    //prepareBulkButton.addEventListener('click', () => {
                    function performBulkTransfer(transferActivated = false, numRows = table.numRows, expectedCount = table.operationInfo.expectedCount, useExpectedCount = false) {

                        console.log("performBulkTransfer")
                        // Create a result container
                        const resultDiv = document.createElement("div");
                        resultDiv.classList.add("query-result", "result-blue");
                        tableColumns.insertBefore(resultDiv, tableColumnsTable);

                        // Replace button content with spinner
                        const originalText = prepareBulkButton.textContent; // Save the original button text
                        prepareBulkButton.textContent = ''; // Clear the button text
                        spinner.style.display = 'inline-block';
                        prepareBulkButton.appendChild(spinner); // Add spinner to the button

                        // Variables to store job data
                        let jobId = '';
                        let numberRecordsProcessed = 0;
                        let totalProcessingTime = 0;
                        let state = '';

                        const checkJobStatus = () => {
                            fetch(`/internal/performBulk?dataset=${datasetName}&table=${table.tableName}`, {
                                method: "GET" // GET request for this endpoint
                            })
                                .then(response => {
                                    if (response.ok) {
                                        // Parse as JSON for successful responses
                                        return response.json().then(data => ({
                                            status: response.status,
                                            body: data
                                        }));
                                    } else {
                                        // Handle non-200 responses as plain text
                                        return response.text().then(text => ({
                                            status: response.status,
                                            body: text
                                        }));
                                    }
                                })
                                .then(({ status, body }) => {
                                    if (status === 200) {
                                        // Extract job data from the response
                                        state = body.state;
                                        jobId = body.id || 'Unknown';
                                        numberRecordsProcessed = body.numberRecordsProcessed || 0;
                                        totalProcessingTime = body.totalProcessingTime || 0;

                                        // Update the query-result row
                                        resultDiv.innerHTML = `
                                        <div>
                                            Preparing bulk transfer for all records except those modified today<br>
                                            Job ID: ${jobId}<br>
                                            Records Processed: ${numberRecordsProcessed}<br>
                                            Processing Time: ${totalProcessingTime} ms<br>
                                            Status: ${state}
                                        </div>
                                        `;

                                        // Check if the job is complete
                                        if (state !== "JobComplete") {
                                            // Retry after 2 seconds if the job is not complete
                                            setTimeout(checkJobStatus, 500);
                                        } else {
                                            // If the job is complete, add the "Transfer records" button
                                            const transferButton = document.createElement('button');
                                            if (table.active) {
                                                transferButton.textContent = 'Transfer records';
                                            } else {
                                                transferButton.textContent = 'Simulate transferring records'
                                            }
                                            transferButton.classList.add('action-button', 'transfer-button', 'bulk-transfer-button');

                                            // Add the transfer button to the query result
                                            resultDiv.appendChild(transferButton);

                                            // Handle Transfer Button Click
                                            transferButton.addEventListener('click', () => {

                                                transferButton.style.display = "none";
                                                const resultDiv = document.createElement("div");
                                                resultDiv.classList.add("query-result", "result-blue");
                                                tableColumns.insertBefore(resultDiv, tableColumnsTable);

                                                // Call /internal/transfer and handle polling
                                                const pollTransferEndpoint = () => {
                                                    fetch(`/internal/transfer?dataset=${datasetName}&table=${table.tableName}`, {
                                                        method: "GET" // GET request for transfer endpoint
                                                    })
                                                        .then(response => {
                                                            return response.text().then(text => ({
                                                                status: response.status,
                                                                body: text
                                                            }));
                                                        })
                                                        .then(({status, body}) => {
                                                            if (status !== 200 && status !== 202) {
                                                                // If the response status is not OK, show the error
                                                                throw new Error(body || 'Unknown error occurred');
                                                            }

                                                            // Update the query result with the transfer report
                                                            resultDiv.innerHTML = `<pre>${body}</pre>`;

                                                            // Check if the response contains Fail or Done
                                                            if (body.includes('Fail') || body.includes('Done')) {
                                                                prepareBulkButton.style.display = 'block';
                                                                resultDiv.classList.remove("result-blue");
                                                                if (body.includes('Fail')) {
                                                                    resultDiv.classList.add("result-red");
                                                                } else {
                                                                    resultDiv.classList.add("result-green");
                                                                }
                                                                // Add spinner to rowCountDiv
                                                                /*
                                                                const originalRowCountText = rowCountDiv.textContent; // Save the original content
                                                                rowCountDiv.textContent = ''; // Clear the content
                                                                const spinner = document.createElement('div');
                                                                spinner.classList.add('spinner-row'); // Add CSS class for styling the spinner
                                                                rowCountDiv.appendChild(spinner); // Add the spinner

                                                                 */
                                                                if (body.includes('Done') && table.active) {
                                                                    // Regular expression to find the number inside "Done (X records processed)"
                                                                    const match = body.match(/Done \((\d+) records processed\)/);

                                                                    const rowCountText = rowCountDiv.textContent
                                                                    const recordsShownMatch = rowCountText.match(/(\d+)\s+records/);

                                                                    if (match && recordsShownMatch) {
                                                                        const numberOfNew = parseInt(match[1], 10); // Extract the first capturing group
                                                                        console.log("Extracted numberOfNew:", numberOfNew);
                                                                        const numberOfShown = parseInt(recordsShownMatch[1], 10);
                                                                        console.log("Extracted numberOfShown:", numberOfShown);
                                                                        if (numberOfNew > 0) {
                                                                            const newExpectedCount = (useExpectedCount ? expectedCount : numberOfShown + numberOfNew);
                                                                            console.log("newExpectedCount: " + newExpectedCount + ", usedExpectedCount " + useExpectedCount + ", expectedCount" + expectedCount)
                                                                            rowCountDiv.textContent = `${newExpectedCount} records`;
                                                                            rowCountDiv.classList.add("updated")
                                                                            fetch(`/internal/storeExpectedCount?dataset=${datasetName}&table=${table.tableName}&count=${newExpectedCount}`, {
                                                                                method: "GET" // GET request for this endpoint
                                                                            }).then(r =>
                                                                                console.log("storeExpectedCount:" + r.status))
                                                                        }
                                                                    } else {
                                                                        console.log("Both matching patterns not found.");
                                                                    }
                                                                }
                                                            } else {
                                                                // Continue polling if not done
                                                                setTimeout(pollTransferEndpoint, 500);
                                                            }
                                                        })
                                                        .catch(error => {
                                                            // Handle errors during the transfer process
                                                            resultDiv.classList.remove("result-blue");
                                                            resultDiv.classList.add("result-red");
                                                            resultDiv.innerHTML = `<div>Fail: ${error.message}</div>`;
                                                            prepareBulkButton.style.display = 'block';
                                                        });
                                                };

                                                // Start polling the transfer endpoint
                                                pollTransferEndpoint();
                                            });

                                            if (transferActivated) {
                                                //Transfer run registered in backend, transfer poll will only give report back - not start new job
                                                transferButton.click()
                                            }

                                            // Restore the button state but hide it
                                            if (prepareBulkButton.contains(spinner)) {
                                                prepareBulkButton.removeChild(spinner); // Remove spinner
                                            }

                                            prepareBulkButton.textContent = originalText;
                                            prepareBulkButton.style.display = 'none';
                                        }
                                    } else {
                                        resultDiv.innerHTML = `Fail: ${body}`;
                                        resultDiv.classList.add("result-red");
                                        if (prepareBulkButton.contains(spinner)) {
                                            prepareBulkButton.removeChild(spinner); // Remove spinner
                                        }
                                        prepareBulkButton.textContent = originalText;
                                    }
                                })
                                .catch(error => {
                                    // Handle errors
                                    resultDiv.innerHTML = `<div>Fail: ${error.message}</div>`;
                                    resultDiv.classList.add("result-red");
                                    if (prepareBulkButton.contains(spinner)) {
                                        prepareBulkButton.removeChild(spinner); // Remove spinner
                                    }
                                    prepareBulkButton.textContent = originalText;
                                });
                        };

                        // Start the job check loop
                        checkJobStatus();
                    }

                    buttonRow.appendChild(testQueryButton);
                    buttonRow.appendChild(spinner);
                    buttonRow.appendChild(prepareBulkButton);

                    tableDetails.appendChild(buttonRow);
                }

                // Table columns (hidden by default)
                const tableColumns = document.createElement("div");
                tableColumns.classList.add("table-columns");

                const tableColumnsTable = document.createElement("table");

                // Add header row
                const headerRow = document.createElement("tr");
                ["BigQuery Field", "Salesforce Field", "Type"].forEach(headerText => {
                    const th = document.createElement("th");
                    th.textContent = headerText;
                    headerRow.appendChild(th);
                });
                tableColumnsTable.appendChild(headerRow);

                // Add column rows
                table.columns.forEach(column => {
                    const row = document.createElement("tr");
                    ["name", "salesforceFieldName", "type"].forEach(key => {
                        const td = document.createElement("td");
                        td.textContent = column[key];
                        row.appendChild(td);
                    });
                    tableColumnsTable.appendChild(row);
                });

                tableColumns.appendChild(tableColumnsTable);
                tableDetails.appendChild(tableColumns);
                tableDiv.appendChild(tableDetails);

                // Toggle visibility of columns on header click
                tableHeader.addEventListener("click", function () {
                    if (tableDetails.style.display === "none" || !tableDetails.style.display) {
                        tableDetails.style.display = "block";
                    } else {
                        tableDetails.style.display = "none";
                    }
                });

                datasetSection.appendChild(tableDiv);

                // Recover backendstate if frontend has been refreshed:
                if (table.operationInfo.preparingBulk) {
                    console.log("Recover state of bulk operation. Transfer activated: " + table.operationInfo.transfering)
                    //                     function performBulkTransfer(transferActivated = false, numRows = table.numRows, expectedCount = table.operationInfo.expectedCount, useExpectedCount = false) {
                    performBulkTransfer(table.operationInfo.transfering, table.numRows, table.operationInfo.expectedCount, true)
                }
            });

            metadataContainer.appendChild(datasetSection);
        });
    }

    function highlightUnmappedFields() {
        // Find all table rows in the metadata table
        const rows = document.querySelectorAll('.table-columns table tr');

        rows.forEach((row) => {
            // Define the cell indices you want to process
            const cellIndices = [0, 1, 2];

            // Iterate over the specified cell indices
            cellIndices.forEach((index) => {
                // Find the cell that contains salesforceFieldName (assuming it's in a fixed column index, e.g., 3)
                const salesforceFieldCell = row.children[index]; // Adjust index based on the column order

                if (salesforceFieldCell && (salesforceFieldCell.textContent.includes("No mapping configured") ||
                    (salesforceFieldCell.textContent.includes("Missing in query")) ||
                    (salesforceFieldCell.textContent.includes("Not existing")) ||
                    (salesforceFieldCell.textContent.includes("Mismatch"))
                )) {
                    // Highlight the row with a light yellow background "Missing in query"

                    const msg = salesforceFieldCell.textContent

                    // Determine if the message contains a variable field name and split accordingly
                    let fieldName = "";
                    let statusText = "";

                    if (msg.includes(" - ")) {
                        // Split the content into the field name and status
                        [fieldName, statusText] = msg.split(" - ", 2); // Splits into two parts: before and after " - "
                    } else {
                        // Default to using the entire content as the status
                        statusText = msg;
                    }

                    row.style.backgroundColor = '#fffde7'; // Light yellow

                    // Clear the existing content
                    salesforceFieldCell.innerHTML = '';

                    // Create a container for both spans
                    const containerSpan = document.createElement('span');
                    containerSpan.style.whiteSpace = 'nowrap'; // Prevent line breaks inside this container

                    // Create a span for the field name
                    const fieldNameSpan = document.createElement('span');
                    fieldNameSpan.textContent = fieldName;
                    fieldNameSpan.classList.add('field-name-text'); // Add a class for the field name

                    // Create a span for the styled status text
                    const statusSpan = document.createElement('span');
                    statusSpan.textContent = statusText;
                    statusSpan.classList.add('no-mapping-text'); // Add a class for styling the status

                    // Append the spans to the container
                    containerSpan.appendChild(fieldNameSpan);

                    // Add a space between the field name and the status for better readability
                    containerSpan.appendChild(document.createTextNode(' '));

                    containerSpan.appendChild(statusSpan);

                    // Append the container to the cell
                    salesforceFieldCell.appendChild(containerSpan);
                }
            })
        });
    }
});

function clearEventListeners(element) {
    const clone = element.cloneNode(true);
    element.parentNode.replaceChild(clone, element);
    return clone;
}
