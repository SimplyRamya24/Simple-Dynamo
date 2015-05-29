package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.Comparator;

/**
 * Created by ramya on 5/2/15.
 */
public class CompareHash implements Comparator<NodeObject> {

    @Override
    public int compare(NodeObject lhs, NodeObject rhs) {
        return lhs.mNodeHash.compareTo(rhs.mNodeHash);
    }
}
