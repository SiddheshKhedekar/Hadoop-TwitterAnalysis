package util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class pair implements WritableComparable<pair> {

	Text term;
	Text neighbour;

	public pair() {
		term = new Text();
		neighbour = new Text();
	}

	public void setTerm(String term) {
		this.term.set(term);
	}

	public void setNeighbour(String neighbour) {
		this.neighbour.set(neighbour);
	}

	public void set(String term, String neighbour) {
		this.term.set(term);
		this.neighbour.set(neighbour);
	}

	public String toString() {
		return term.toString() + "," + neighbour.toString();
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		term.readFields(arg0);
		neighbour.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		term.write(arg0);
		neighbour.write(arg0);
	}

	public Text getTerm() {
		return term;
	}

	public void setTerm(Text term) {
		this.term = term;
	}

	public Text getNeighbour() {
		return neighbour;
	}

	public void setNeighbour(Text neighbour) {
		this.neighbour = neighbour;
	}

	@Override
	public int compareTo(pair otherPair) {
		int cmp = this.term.compareTo(otherPair.getTerm());
		if (cmp == 0) {
			return this.neighbour.compareTo(otherPair.getNeighbour());
		} else {
			return cmp;
		}

	}

	// public boolean equals(pair otherPair) {
	// System.out.println("pair term comp :: " +
	// this.term.equals(otherPair.getTerm()));
	// System.out.println("pair neighbour comp :: " +
	// this.neighbour.equals(otherPair.getNeighbour()));
	// return (this.term.equals(otherPair.getTerm()) && this.neighbour
	// .equals(otherPair.getNeighbour()));
	// }

	public boolean equals(Object obj) {
		boolean eqTerm = false, eqNeg = false;
		if (obj instanceof pair) {
			pair otherPair = (pair) obj;
			eqTerm = this.term.equals(otherPair.getTerm());
			eqNeg = this.neighbour.equals(otherPair.getNeighbour());
		}
		return eqTerm && eqNeg;

	}

	public int hashCode() {
		return term.hashCode() * 163 + neighbour.hashCode();
	}

}
