package largespace.clustering;

import largespace.business.Options;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class Cluster {
    public final List<Query> points = new ArrayList<>();


    public void expand(Query point, List<Query> neighbors, List<Query> data, boolean[] visited, boolean[] isClusterMember, Options opt) {
        points.add(point);
        isClusterMember[point.id] = true;

        ListIterator<Query> it = neighbors.listIterator();
        while (it.hasNext()) {
            Query current = it.next();
            int curID = current.id;

            if (current != point
                    && !visited[curID]
                    && !isClusterMember[curID] //Uncomment to avoid comparing to already clustered queries.
                    && !current.fromTables.equals(point.fromTables) // Exact copies are ignored.
                    && !current.whereClausesTerms.equals(point.whereClausesTerms)
                    ) {
                visited[curID] = true;
                List<Query> curNeighbors = current.region(data, visited, isClusterMember, opt);   // Added Visited and Is cluster to region to avoid Comparisons

                if (curNeighbors.size() >= opt.MIN_PTS) {
                    for (Query temp : curNeighbors) {
                        int tempID = temp.id;
                        if (!isClusterMember[tempID]) {
                            isClusterMember[tempID] = true;
                            points.add(temp);
                            it.add(temp);
                        }
                    }
                }
            }

            if (!isClusterMember[curID]) {
                points.add(current);
                isClusterMember[curID] = true;
            }
        }
    }
}