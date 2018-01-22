### Test machine

* MacBook Pro (Retina, 15-inch, Mid 2015)
* 2,2 GHz Intel Core i7
* 16GB RAM
* SSD

* postgres 10.1 installed locally with default settings (i think)
    * shared_buffers=128MB
    * work_mem=4MB
    * no cheats like fsync=off were used

### Prerequisites

* libpq as a dependency (`go get github.com/lib/pq`)
* You'd need to create a database and change the hardcoded string in the `main.go` file accordingly: `connStr := "user=kolombet dbname=exchange sslmode=disable"`

### Difference from the source project:

* Orders are generated randomly at runtime (with a fixed seed). By default 100k orders are generated.
* `Cancel` operation is no more simulated, and order is simply treated as cancelled if it has `price=0`. There is small chance that some orders will be generated as cancelled orders (with price=0).
* As cancellation of orders is some sort of a business logic thing, we will not take difficult cases like (what if order is cancelled after being matched) into consideration, as I think its out of scope of a simple backend benchmark test.

### Database schema layout:

See `db.go` source file for more details.

We create two tables: `orders` and `deals`, pretty self-explanatory.
Orders table has all the columns of a test feed input, but also a `blocked_size` column, which I'll explain later.
Deals table just contains matches (order_id of a sale-order and order_id of a buy-order), and the exact price and size of a deal.

For simplicity we do not emulate "charging/withdrawal" operations, we assume that some separate process might look in the deals table to see which operations it need to perform. So, out of scope of this project.

### The workflow

Engine selects all the orders with LIMIT `maxNumOrders` and selects them FOR UPDATE, to be safe from concurrency problems. Thus, one order can be processed only by one Engine at one time.

After that the matching algo is run (I didn't change it, also I fixed the `e.Reset()` method as it was not properly resetting some states between consecutive runs.

After each selected order passed through the matching algo, we have a list of `deals` which we need to persist to the database.

### Persisting

Persisting is done in transaction. `deals` table being populated as fast as possible (using sql `COPY` statement). After that for each deal both participating orders are updated, and have their "blocked_size" set accordingly.

Blocked size is an amount of currency which participates in the deal, it can be less than order size.

For example if we have an order SELL with size=600 and three BUY orders with sizes = 200, 300, 400, then SELL order will have blocked_size=600, and buy orders will have blocked_size = 200, 300 and 100 correspondingly.
Orders are being served on the "first come first go" basis (ORDER BY id asc)

### Results

On my local machine I get the following results:

```
[engine] mean(latency) = 0.00, sd(latency) = 0.00
[fetch] mean(latency) = 0.41, sd(latency) = 0.44
[persist] mean(latency) = 1.57, sd(latency) = 1.66
[total] 42768.3 orders per second
```

So generally somewhere between 42k-49k orders per second are processed.

Code is concurrency-safe and can be horizontally scaled pretty easily i think.



