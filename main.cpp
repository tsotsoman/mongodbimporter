#include "pugixml.hpp"

#include <iostream>

#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/types.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>

using namespace pugi;
using namespace bsoncxx::builder;
using bsoncxx::builder::basic::kvp;

#include <nlohmann/json.hpp>
#include <nlohmann/fifo_map.hpp>
using namespace nlohmann;

// A workaround to give to use fifo_map as map, we are just ignoring the 'less' compare
template<class K, class V, class dummy_compare, class A>
using my_workaround_fifo_map = fifo_map<K, V, fifo_map_compare<K>, A>;
using my_json = basic_json<my_workaround_fifo_map>;

pugi::xml_document doc;

bool load_xml(const std::string& xml_path) {	
	pugi::xml_parse_result result = doc.load_file(xml_path.c_str());
	std::cout << "Parsing " << xml_path << ": " << result.description() << std::endl;
	return result;
}

// parse top level (build metadata document)
void import_toplevel_xml(mongocxx::client& conn) {
	xml_node db_root_node = doc.first_child();
	bsoncxx::builder::stream::document meta{};
	for (pugi::xml_attribute attr : db_root_node.attributes())
	{
		meta << std::string(attr.name()) << std::string(attr.value());
	}
	auto metadata_collection = conn["db_xml"]["metadata"];
	metadata_collection.replace_one(stream::document{} << stream::finalize,  meta.view());
}

my_json import_sublevel_xml_impl(xml_node& curr_node, const std::string& curr_path) {
	my_json jsonDoc;
	if (curr_node.name() == std::string("att")) {
		const std::string sID = curr_node.attribute("id").as_string();
		return import_sublevel_xml_impl(curr_node.first_child(),
										curr_path + "/" + sID);
	}
	else if (curr_node.name() == std::string("obj")) {
		jsonDoc["path"] = curr_path;
		for (xml_attribute att : curr_node.attributes()) {
			jsonDoc[att.name()] = att.as_string();
		}
		for (xml_node inner_att_node : curr_node.children()) {
			assert(inner_att_node.attribute("id"));
			const std::string sID = inner_att_node.attribute("id").as_string();
			jsonDoc.push_back(my_json::object_t::value_type(sID,
															import_sublevel_xml_impl(inner_att_node,
																					 curr_path)));
			auto str = jsonDoc.dump();
		}
	}
	else if (curr_node.name() == std::string("val")) {
		jsonDoc["path"]  = curr_path;
		jsonDoc["value"] = curr_node.child_value();
	}
	else if (curr_node.name() == std::string("list")) {
		jsonDoc["path"] = curr_path;
		// open an JSON array
		std::string arrayName = curr_node.attribute("key").as_string("unnamed");
		if (arrayName.empty())
			arrayName = "unnamed"; // bson won't accept empty array fields
		jsonDoc[arrayName] = my_json::array();
		//auto itInserted = jsonDoc.insert(jsonDoc.end(), {[]});
		for (xml_node inner_att_node : curr_node.children()) {
			jsonDoc[arrayName].push_back({ import_sublevel_xml_impl(inner_att_node, curr_path) });
			auto str = jsonDoc.dump();
		}
	}
	auto str = jsonDoc.dump();
	return jsonDoc;
}

std::string import_sublevel_xml(xml_node& curr_node, const std::string& curr_path) {
	assert(curr_node.name() == std::string("att"));
	assert(curr_node.attribute("id"));
	my_json jsonDoc;
	jsonDoc[curr_node.attribute("id").as_string()] = import_sublevel_xml_impl(curr_node, curr_path);
	auto tmp = jsonDoc.dump();
	return tmp;
}

int main(int, char**) {
	// connect to mongodb
	mongocxx::instance inst{};
	mongocxx::client conn{ mongocxx::uri{} };

	// load db.xml
	if (!load_xml("C:\\ProgramData\\CTERA\\CTERA Agent\\db.xml")) {
		std::cout << "Failed parsing xml" << std::endl;
		return 0;
	}

	import_toplevel_xml(conn);

	for (xml_node toplevel_node : doc.first_child().first_child().children()) {
		assert(toplevel_node.name() == std::string("att"));
		assert(!std::string(toplevel_node.attribute("id").value()).empty());

		std::cout << toplevel_node.name() << ":" << toplevel_node.attribute("id").value() << std::endl;
		// create collection
		auto coll = conn["db_xml"][toplevel_node.attribute("id").value()];
		coll.drop();

		try {
			coll.insert_one(bsoncxx::from_json(import_sublevel_xml(toplevel_node, "")));
		}
		catch (std::exception& e) {
			std::cerr << e.what() << std::endl;
		}
	}
	system("@pause");
}