#!/usr/bin/env python3
"""
RDF Parser Demonstration

This script demonstrates how to parse downloaded PubChem RDF files
and extract useful information for building knowledge graphs.
"""

import os
import sys
import gzip
import logging
from pathlib import Path
from typing import Iterator, Tuple, List, Dict, Any

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

try:
    import rdflib
    from rdflib import Graph, URIRef, Literal, Namespace
    from rdflib.namespace import RDF, RDFS, OWL
except ImportError:
    print("Error: rdflib not installed. Install with: pip install rdflib")
    sys.exit(1)

try:
    import networkx as nx
    import matplotlib.pyplot as plt
except ImportError:
    print("Warning: NetworkX and matplotlib not available for graph visualization")
    nx = None
    plt = None


# Define common PubChem namespaces
PUBCHEM = Namespace("http://rdf.ncbi.nlm.nih.gov/pubchem/")
PUBCHEM_COMPOUND = Namespace("http://rdf.ncbi.nlm.nih.gov/pubchem/compound/")
PUBCHEM_SUBSTANCE = Namespace("http://rdf.ncbi.nlm.nih.gov/pubchem/substance/")
OBO = Namespace("http://purl.obolibrary.org/obo/")
CHEMINF = Namespace("http://semanticscience.org/resource/")


class PubChemRDFParser:
    """Parser for PubChem RDF data files."""
    
    def __init__(self, data_directory: str = "data/pubchem_rdf"):
        """Initialize the parser with the data directory."""
        self.data_directory = Path(data_directory)
        self.logger = logging.getLogger(__name__)
        
        # Initialize RDF graph
        self.graph = Graph()
        
        # Bind common namespaces
        self._bind_namespaces()
    
    def _bind_namespaces(self):
        """Bind common namespaces to the graph."""
        self.graph.bind("pubchem", PUBCHEM)
        self.graph.bind("compound", PUBCHEM_COMPOUND)
        self.graph.bind("substance", PUBCHEM_SUBSTANCE)
        self.graph.bind("obo", OBO)
        self.graph.bind("cheminf", CHEMINF)
        self.graph.bind("rdf", RDF)
        self.graph.bind("rdfs", RDFS)
        self.graph.bind("owl", OWL)
    
    def find_rdf_files(self, subdirectory: str = None, limit: int = None) -> List[Path]:
        """
        Find RDF files in the data directory.
        
        Args:
            subdirectory: Specific subdirectory to search (e.g., 'compound')
            limit: Maximum number of files to return
            
        Returns:
            List of RDF file paths
        """
        search_dir = self.data_directory
        if subdirectory:
            search_dir = search_dir / subdirectory
        
        if not search_dir.exists():
            self.logger.warning(f"Directory not found: {search_dir}")
            return []
        
        # Find .ttl and .ttl.gz files
        rdf_files = []
        for pattern in ["*.ttl", "*.ttl.gz", "*.rdf", "*.rdf.gz"]:
            rdf_files.extend(search_dir.glob(pattern))
        
        self.logger.info(f"Found {len(rdf_files)} RDF files in {search_dir}")
        
        if limit:
            rdf_files = rdf_files[:limit]
            self.logger.info(f"Limited to {len(rdf_files)} files")
        
        return rdf_files
    
    def parse_file(self, file_path: Path, format: str = "turtle") -> bool:
        """
        Parse a single RDF file and add to the graph.
        
        Args:
            file_path: Path to the RDF file
            format: RDF format (turtle, xml, n3, etc.)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info(f"Parsing file: {file_path}")
            
            # Handle compressed files
            if file_path.suffix == '.gz':
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    self.graph.parse(f, format=format)
            else:
                self.graph.parse(str(file_path), format=format)
            
            self.logger.info(f"Successfully parsed {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to parse {file_path}: {e}")
            return False
    
    def parse_multiple_files(self, file_paths: List[Path], format: str = "turtle") -> int:
        """
        Parse multiple RDF files.
        
        Args:
            file_paths: List of file paths to parse
            format: RDF format
            
        Returns:
            Number of successfully parsed files
        """
        successful = 0
        total = len(file_paths)
        
        for i, file_path in enumerate(file_paths, 1):
            self.logger.info(f"Processing file {i}/{total}: {file_path.name}")
            if self.parse_file(file_path, format):
                successful += 1
            
            # Progress indicator
            if i % 10 == 0:
                self.logger.info(f"Progress: {i}/{total} files processed")
        
        self.logger.info(f"Parsed {successful}/{total} files successfully")
        return successful
    
    def get_graph_statistics(self) -> Dict[str, Any]:
        """Get statistics about the current graph."""
        stats = {
            'total_triples': len(self.graph),
            'subjects': len(set(self.graph.subjects())),
            'predicates': len(set(self.graph.predicates())),
            'objects': len(set(self.graph.objects())),
        }
        
        # Count different types of entities
        compound_count = len(list(self.graph.subjects(RDF.type, PUBCHEM_COMPOUND)))
        substance_count = len(list(self.graph.subjects(RDF.type, PUBCHEM_SUBSTANCE)))
        
        stats.update({
            'compounds': compound_count,
            'substances': substance_count
        })
        
        return stats
    
    def query_compounds(self, limit: int = 10) -> List[Tuple[str, str]]:
        """
        Query for compound information.
        
        Args:
            limit: Maximum number of results
            
        Returns:
            List of tuples (compound_uri, label)
        """
        query = f"""
        PREFIX pubchem: <{PUBCHEM}>
        PREFIX rdfs: <{RDFS}>
        
        SELECT ?compound ?label
        WHERE {{
            ?compound a pubchem:Compound .
            OPTIONAL {{ ?compound rdfs:label ?label }}
        }}
        LIMIT {limit}
        """
        
        results = []
        for row in self.graph.query(query):
            compound_uri = str(row.compound)
            label = str(row.label) if row.label else "No label"
            results.append((compound_uri, label))
        
        return results
    
    def query_compound_properties(self, compound_uri: str) -> Dict[str, List[str]]:
        """
        Query properties for a specific compound.
        
        Args:
            compound_uri: URI of the compound
            
        Returns:
            Dictionary of properties and values
        """
        query = f"""
        SELECT ?property ?value
        WHERE {{
            <{compound_uri}> ?property ?value .
        }}
        """
        
        properties = {}
        for row in self.graph.query(query):
            prop = str(row.property)
            value = str(row.value)
            
            if prop not in properties:
                properties[prop] = []
            properties[prop].append(value)
        
        return properties
    
    def extract_relationships(self) -> List[Tuple[str, str, str]]:
        """
        Extract relationships between entities.
        
        Returns:
            List of tuples (subject, predicate, object)
        """
        relationships = []
        
        # Limit to avoid memory issues with large graphs
        count = 0
        for s, p, o in self.graph:
            if count >= 1000:  # Limit for demonstration
                break
            relationships.append((str(s), str(p), str(o)))
            count += 1
        
        return relationships
    
    def create_networkx_graph(self, max_nodes: int = 100) -> 'nx.Graph':
        """
        Create a NetworkX graph from RDF data.
        
        Args:
            max_nodes: Maximum number of nodes to include
            
        Returns:
            NetworkX graph object
        """
        if nx is None:
            raise ImportError("NetworkX not available")
        
        G = nx.Graph()
        node_count = 0
        
        for s, p, o in self.graph:
            if node_count >= max_nodes:
                break
            
            # Only include URIRefs as nodes (skip literals for simplicity)
            if isinstance(s, URIRef) and isinstance(o, URIRef):
                G.add_node(str(s), type='subject')
                G.add_node(str(o), type='object')
                G.add_edge(str(s), str(o), relation=str(p))
                node_count += 2
        
        return G
    
    def visualize_graph(self, G: 'nx.Graph', output_file: str = "pubchem_graph.png"):
        """
        Visualize the NetworkX graph.
        
        Args:
            G: NetworkX graph
            output_file: Output file path
        """
        if plt is None:
            raise ImportError("Matplotlib not available")
        
        plt.figure(figsize=(12, 8))
        pos = nx.spring_layout(G, k=1, iterations=50)
        
        # Draw nodes
        nx.draw_networkx_nodes(G, pos, node_size=50, node_color='lightblue', alpha=0.7)
        
        # Draw edges
        nx.draw_networkx_edges(G, pos, alpha=0.5, edge_color='gray')
        
        # Add labels for a few nodes
        labels = {node: node.split('/')[-1][:10] for node in list(G.nodes())[:20]}
        nx.draw_networkx_labels(G, pos, labels, font_size=8)
        
        plt.title("PubChem Knowledge Graph Sample")
        plt.axis('off')
        plt.tight_layout()
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        plt.show()
        
        print(f"Graph visualization saved to: {output_file}")
    
    def save_graph(self, output_file: str, format: str = "turtle"):
        """
        Save the current graph to a file.
        
        Args:
            output_file: Output file path
            format: RDF serialization format
        """
        try:
            self.graph.serialize(destination=output_file, format=format)
            self.logger.info(f"Graph saved to {output_file} in {format} format")
        except Exception as e:
            self.logger.error(f"Failed to save graph: {e}")


def setup_logging():
    """Set up logging for the demo."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def main():
    """Main demonstration function."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    print("PubChem RDF Parser Demonstration")
    print("=" * 50)
    
    # Initialize parser
    parser = PubChemRDFParser()
    
    # Check if data directory exists
    if not parser.data_directory.exists():
        print(f"Data directory not found: {parser.data_directory}")
        print("Please run the downloader first to get RDF data.")
        return
    
    # Find some RDF files to parse (limit for demo)
    print("\n1. Searching for RDF files...")
    rdf_files = parser.find_rdf_files(subdirectory="compound", limit=5)
    
    if not rdf_files:
        print("No RDF files found. Please download some data first.")
        return
    
    print(f"Found {len(rdf_files)} files to parse")
    
    # Parse the files
    print("\n2. Parsing RDF files...")
    successful = parser.parse_multiple_files(rdf_files)
    print(f"Successfully parsed {successful} files")
    
    if successful == 0:
        print("No files were parsed successfully.")
        return
    
    # Show graph statistics
    print("\n3. Graph Statistics:")
    stats = parser.get_graph_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value:,}")
    
    # Query for compounds
    print("\n4. Sample Compounds:")
    compounds = parser.query_compounds(limit=5)
    for i, (uri, label) in enumerate(compounds, 1):
        print(f"  {i}. {uri}")
        print(f"     Label: {label}")
    
    # Show properties for first compound
    if compounds:
        print("\n5. Properties for first compound:")
        compound_uri = compounds[0][0]
        properties = parser.query_compound_properties(compound_uri)
        
        for prop, values in list(properties.items())[:5]:  # Show first 5 properties
            print(f"  {prop}:")
            for value in values[:3]:  # Show first 3 values
                print(f"    - {value}")
    
    # Create NetworkX graph
    if nx is not None:
        print("\n6. Creating NetworkX graph...")
        try:
            G = parser.create_networkx_graph(max_nodes=50)
            print(f"NetworkX graph created with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
            
            # Visualize if matplotlib is available
            if plt is not None:
                print("Creating visualization...")
                parser.visualize_graph(G)
            
        except Exception as e:
            logger.error(f"Failed to create NetworkX graph: {e}")
    
    # Save a sample of the graph
    print("\n7. Saving sample graph...")
    try:
        output_file = "sample_pubchem_graph.ttl"
        parser.save_graph(output_file)
        print(f"Sample graph saved to: {output_file}")
    except Exception as e:
        logger.error(f"Failed to save graph: {e}")
    
    print("\nDemonstration complete!")
    print("This shows basic RDF parsing capabilities.")
    print("For production use, consider using a triple store for better performance.")


if __name__ == "__main__":
    main() 