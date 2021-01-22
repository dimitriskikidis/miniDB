import select
import socket
from tabulate import tabulate
from database import Database

def show_to_string(self):
    '''
    Return the table as a string in a preety form.
    '''
    result = f"\n## {self._name} ##\n"
    # headers -> "column name (column type)"
    headers = [f"{col} ({tp.__name__})" for col, tp in zip(self.column_names, self.column_types)]
    if self.pk_idx is not None:
        # table has a primary key, add PK next to the appropriate column
        headers[self.pk_idx] = headers[self.pk_idx] + " #PK#"
    # detect the rows that are not full of nones (these rows have been deleted)
    # if we dont skip these rows, the returning table has empty rows at the deleted positions
    non_none_rows = [row for row in self.data if any(row)]
    # print using tabulate
    result += tabulate(non_none_rows[:], headers = headers) + "\n\n"
    return result

def handle_query(query):
    '''
    Handle the client's query. This function can be extended to fully support SQL statements.
    Now it only supports 'select * from table_name' statements.
    '''
    # Extract table name from query.
    table_name = (query[query.find("from") + len("from"):]).strip()
    # If it exists, send the table in a preety form, else send that the table doesn't exist.
    if table_name in db.tables:
        result = show_to_string(db.tables[table_name])
    else:
        result = f"\nTable '{table_name}' doesn't exist.\n\n"
    return result + chr(6)


# Database to load.
db = Database("vsmdb", load = True)
# Port number.
PORT = 5050
# Socket creation.
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# Make the server socket non-blocking.
server.setblocking(0)
# Bind server to IP and Port.
server.bind(("", PORT))
# Start listening for requests.
server.listen(5)
print("\n[Server] Server started.")
print("\n[Server] Active clients: 0.")

# The list of all the sockets we want to read from.
# It also contains the server socket and when it's readable it means it can accept a new client.
inputs = [server]
# The list of all the sockets we want to send a result to.
outputs = []
# The dictionary of all the queries we are receiving.
queries = {}
# The dictionary of all the results we are sending to clients.
results = {}
# The dictionary of all the addresses of the connected clients.
addresses = {}

try:
    while inputs:
        # Use select() that returns 3 lists. The 'readable' list contains the sockets we can start reading from.
        # It also may contain the server socket when it is ready to accept a new client. The 'writable' list contains
        # the sockets we can start sending a result to. The 'exceptional' list contains the sockets that have an error.
        # The last parameter of select.select() is a 60 second timeout. If it hasn't returned the 3 lists in a minute
        # then it stops trying to read the sockets and returns 3 empty lists.
        #
        # select.select() parameters:
        # inputs: list of all sockets we want to read from + the server (to be able to accept a new client).
        # outputs: list of all sockets we want to sent a result to.
        # inputs + outputs: list of all sockets we are connected to(every socket is in one of the two lists).
        # 60: a timeout of one minute. If the time expires without a result, 3 empty lists are returned.
        readable, writable, exceptional = select.select(
            inputs, outputs, inputs + outputs, 60)
        # For every readable socket.
        for s in readable:
            # If it's the server accept a new request.
            if s is server:
                # Accept a request.
                connection, address = s.accept()
                # Make the new socket non-blocking.
                connection.setblocking(0)
                # Append the new socket to the inputs list, initialize it's query and address in the dictionaries.
                inputs.append(connection)
                queries[connection] = ""
                addresses[connection] = address
                print(f"\n[Server] Connected to |{address[0]} : {address[1]}|.")
                print(f"\n[Server] Active clients: {len(inputs) + len(outputs) - 1}.")
            # If it's not the server then start reading from it.
            else:
                # Receive the client's query.
                received = s.recv(512).decode("utf-8")
                # If we received > 0 bytes.
                if received:
                    queries[s] += received
                    # If we have received the entire query the last character is a non-printable character
                    # that indicates the end of the message.
                    if received[-1] == chr(6):
                        # Remove the non-printable character from the query.
                        queries[s] = queries[s][:-1]
                        query = queries[s]
                        print(f"\n[Server] Received the query '{query}' from |{addresses[s][0]} : {addresses[s][1]}|.")
                        # The terminating condition for the connection is: query = "exit".
                        if query == "exit":
                            print(f"\n[Server] Disconnected from |{addresses[s][0]} : {addresses[s][1]}|.")
                            # Close the socket and remove the socket's existing information from the lists/dictionaries.
                            s.close()
                            inputs.remove(s)
                            del queries[s]
                            del addresses[s]
                            print(f"\n[Server] Active clients: {len(inputs) + len(outputs) - 1}.")
                        else:
                            # Return the query's result and send it to the client.
                            results[s] = handle_query(query)
                            # Remove the socket from the inputs and append it to the outputs as we want to send it the result.
                            inputs.remove(s)
                            outputs.append(s)
                # If we received 0 bytes the socket has closed.
                else:
                    print(f"\n[Server] Error: Lost connection to |{addresses[s][0]} : {addresses[s][1]}|.")
                    # Close the socket and remove the socket's existing information from the lists/dictionaries.
                    s.close()
                    inputs.remove(s)
                    del queries[s]
                    del addresses[s]
                    print(f"\n[Server] Active clients: {len(inputs) + len(outputs) - 1}.")

        # For every writeable socket.
        for s in writable:
            # Start sending the result to the socket and get the number of bytes sent.
            sent_bytes = s.send(str(results[s]).encode("utf-8"))
            # If we sent 0 bytes the socket has closed.
            if not sent_bytes:
                print(f"\n[Server] Error: Lost connection to |{addresses[s][0]} : {addresses[s][1]}|.")
                # Close the socket and remove the socket's existing information from the lists/dictionaries.
                s.close()
                outputs.remove(s)
                del results[s]
                del addresses[s]
            # If we sent > 0 bytes.
            else:
                # Remove the sent part from the dictionary with the results that need to be sent.
                results[s] = results[s][sent_bytes:]
                # If the remaining result is the empty string it means the result was sent successfully.
                if not results[s]:
                    print(f"\n[Server] Sent the result for the query '{queries[s]}' to |{addresses[s][0]} : {addresses[s][1]}|.")
                    # Remove the socket from the outputs and append it to the inputs as we want to receive a new query from it.
                    inputs.append(s)
                    outputs.remove(s)
                    # Delete the just sent result from the dictionary and initialize the socket's received query from the server.
                    del results[s]
                    queries[s] = ""

        # For every socket that has an error remove all of its information and close the socket.
        for s in exceptional:
            if s in inputs:
                inputs.remove(s)
            if s in outputs:
                outputs.remove(s)
            if s in queries:
                del queries[s]
            if s in results:
                del results[s]
            if s in addresses:
                del addresses[s]
            s.close()
except:
    server.close()
    print("\n[Server] Server stopped.")
