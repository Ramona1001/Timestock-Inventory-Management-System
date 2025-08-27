import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:http/http.dart' as http;
import 'package:flutter_secure_storage/flutter_secure_storage.dart';
import 'package:dropdown_search/dropdown_search.dart';

void main() {
  runApp(MyApp());
}

const baseUrl = "http://192.168.1.60:8000";

class MyApp extends StatelessWidget {
  final storage = FlutterSecureStorage();

  MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'TimeStock Mobile App',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        primarySwatch: Colors.blue,
        scaffoldBackgroundColor: Colors.grey[100],
        elevatedButtonTheme: ElevatedButtonThemeData(
          style: ElevatedButton.styleFrom(
            padding: EdgeInsets.symmetric(vertical: 20, horizontal: 24),
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(10),
            ),
          ),
        ),
      ),
      home: LoginPage(),
    );
  }
}

class LoginPage extends StatefulWidget {
  const LoginPage({super.key});

  @override
  State<LoginPage> createState() => _LoginPageState();
}

class _LoginPageState extends State<LoginPage> {
  final emailController = TextEditingController();
  final passwordController = TextEditingController();
  final storage = FlutterSecureStorage();
  bool _obscurePassword = true;
  bool isLoading = false;

  Future<void> login() async {
    setState(() => isLoading = true);
    try {
      final response = await http.post(
        Uri.parse('$baseUrl/login'),
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Accept': 'application/json',
        },
        body: {
          'email': emailController.text,
          'password': passwordController.text,
        },
      );

      if (response.statusCode == 200) {
        final data = json.decode(response.body);

        if (data['access_token'] != null) {
          await storage.write(key: 'token', value: data['access_token']);
          Navigator.pushReplacement(
            context,
            MaterialPageRoute(builder: (context) => StockPage()),
          );
        } else {
          _showErrorDialog(
            "Login Failed",
            "Access token not found in server response.",
          );
        }
      } else {
        _showErrorDialog(
          "Login Failed",
          "Invalid credentials or server error.",
        );
      }
    } catch (e) {
      _showErrorDialog("Network Error", "Failed to connect to the server.\n$e");
    } finally {
      setState(() => isLoading = false);
    }
  }

  void _showErrorDialog(String title, String message) {
    showDialog(
      context: context,
      builder: (_) => AlertDialog(
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(12)),
        title: Text(title),
        content: Text(message),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context),
            child: Text("OK"),
          ),
        ],
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: Center(
        child: SingleChildScrollView(
          padding: EdgeInsets.all(10),
          child: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            children: [
              Icon(Icons.inventory_2, size: 80, color: Colors.blue),
              SizedBox(height: 10),
              Text(
                "TimeStock Mobile App",
                style: TextStyle(
                  fontSize: 24,
                  fontWeight: FontWeight.bold,
                  color: Colors.blue[800],
                ),
              ),
              SizedBox(height: 40),
              TextField(
                controller: emailController,
                decoration: InputDecoration(
                  labelText: 'Email',
                  prefixIcon: Icon(Icons.email_outlined),
                  border: OutlineInputBorder(
                    borderRadius: BorderRadius.circular(12),
                  ),
                ),
              ),
              SizedBox(height: 16),
              TextField(
                controller: passwordController,
                decoration: InputDecoration(
                  labelText: 'Password',
                  prefixIcon: Icon(Icons.lock_outline),
                  suffixIcon: IconButton(
                    icon: Icon(
                      _obscurePassword
                          ? Icons.visibility_off
                          : Icons.visibility,
                    ),
                    onPressed: () {
                      setState(() {
                        _obscurePassword = !_obscurePassword;
                      });
                    },
                  ),
                  border: OutlineInputBorder(
                    borderRadius: BorderRadius.circular(12),
                  ),
                ),
                obscureText: _obscurePassword,
              ),
              SizedBox(height: 24),
              isLoading
                  ? CircularProgressIndicator()
                  : ElevatedButton.icon(
                      onPressed: login,
                      icon: Icon(Icons.login),
                      label: Text("Login"),
                    ),
            ],
          ),
        ),
      ),
    );
  }
}

class StockPage extends StatefulWidget {
  const StockPage({super.key});

  @override
  State<StockPage> createState() => _StockPageState();
}

class _StockPageState extends State<StockPage> {
  final storage = FlutterSecureStorage();
  List<Map<String, dynamic>> materials = [];
  bool isLoading = true;

  @override
  void initState() {
    super.initState();
    fetchMaterials();
  }

  Future<void> fetchMaterials() async {
    setState(() => isLoading = true);
    try {
      final token = await storage.read(key: 'token');
      final response = await http.get(
        Uri.parse('$baseUrl/api/materials'),
        headers: {
          'Authorization': 'Bearer $token',
          'Accept': 'application/json',
        },
      );

      if (response.statusCode == 200) {
        final List<dynamic> data = json.decode(response.body);
        setState(() {
          materials = data.map((mat) {
            return {
              "id": mat["material_id"],
              "name": mat["item_name"],
              "stock": mat["current_stock"],
            };
          }).toList();
          isLoading = false;
        });
      } else {
        throw Exception("Failed to load materials");
      }
    } catch (e) {
      setState(() => isLoading = false);
      _showErrorDialog("Error", e.toString());
    }
  }

  void _showErrorDialog(String title, String message) {
    showDialog(
      context: context,
      builder: (_) => AlertDialog(
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(12)),
        title: Text(title),
        content: Text(message),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context),
            child: Text("OK"),
          ),
        ],
      ),
    );
  }

  Future<void> _signOut() async {
    await storage.delete(key: 'token');
    Navigator.pushAndRemoveUntil(
      context,
      MaterialPageRoute(builder: (context) => LoginPage()),
      (route) => false,
    );
  }

  void _addStockAction(Map<String, dynamic> material) async {
    final result = await Navigator.push<bool>(
      context,
      MaterialPageRoute(builder: (context) => AddStockPage(material: material)),
    );
    if (result == true) {
      fetchMaterials();
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text("Stock updated")));
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text('TimeStock Mobile App'),
        centerTitle: true,
        actions: [
          IconButton(
            icon: Icon(Icons.logout),
            tooltip: "Sign Out",
            onPressed: _signOut,
          ),
        ],
      ),
      body: isLoading
          ? Center(child: CircularProgressIndicator())
          : ListView.builder(
              padding: EdgeInsets.all(12),
              itemCount: materials.length,
              itemBuilder: (context, index) {
                final mat = materials[index];
                return Card(
                  shape: RoundedRectangleBorder(
                    borderRadius: BorderRadius.circular(12),
                  ),
                  child: ListTile(
                    title: Text(mat["name"]),
                    subtitle: Text("Current Stock: ${mat["stock"]}"),
                    trailing: ElevatedButton(
                      onPressed: () => _addStockAction(mat),
                      child: Text("Add Stock"),
                    ),
                  ),
                );
              },
            ),
    );
  }
}

class AddStockPage extends StatefulWidget {
  final Map<String, dynamic> material;
  const AddStockPage({super.key, required this.material});

  @override
  State<AddStockPage> createState() => _AddStockPageState();
}

class _AddStockPageState extends State<AddStockPage> {
  final quantityController = TextEditingController();
  final storage = FlutterSecureStorage();

  List<dynamic> suppliers = [];
  dynamic selectedSupplier;

  @override
  void initState() {
    super.initState();
    fetchSuppliers();
  }

  Future<void> fetchSuppliers() async {
    final token = await storage.read(key: 'token');
    final response = await http.get(
      Uri.parse('$baseUrl/api/suppliers'),
      headers: {'Authorization': 'Bearer $token', 'Accept': 'application/json'},
    );

    if (response.statusCode == 200) {
      setState(() {
        suppliers = json.decode(response.body);
      });
    } else {
      throw Exception("Failed to load suppliers");
    }
  }

  Future<void> submitStock() async {
    if (selectedSupplier == null || quantityController.text.isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text("Please select a supplier and enter quantity")),
      );
      return;
    }

    final token = await storage.read(key: 'token');
    final response = await http.post(
      Uri.parse('$baseUrl/api/stock-materials'),
      headers: {
        'Authorization': 'Bearer $token',
        'Content-Type': 'application/json',
      },
      body: json.encode({
        "stock_type_id": "STT001",
        "supplier_id": selectedSupplier["id"],
        "items": [
          {
            "material_id": widget.material["id"],
            "quantity": int.parse(quantityController.text),
          },
        ],
      }),
    );

    if (response.statusCode == 200) {
      Navigator.pop(context, true);
    } else {
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text("Failed to add stock")));
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: Text("Add Stock")),
      body: Padding(
        padding: EdgeInsets.all(16),
        child: Column(
          children: [
            Text(
              "Material: ${widget.material["name"]}",
              style: TextStyle(fontWeight: FontWeight.bold, fontSize: 18),
            ),
            Text("Current Stock: ${widget.material["stock"]}"),
            SizedBox(height: 16),
            DropdownSearch<dynamic>(
              items: suppliers,
              itemAsString: (supplier) => supplier?["contact_name"] ?? "",
              selectedItem: selectedSupplier,
              popupProps: PopupProps.menu(
                showSearchBox: true,
                searchFieldProps: TextFieldProps(
                  decoration: InputDecoration(
                    hintText: "Search supplier...",
                    contentPadding: EdgeInsets.all(8),
                  ),
                ),
              ),
              dropdownDecoratorProps: DropDownDecoratorProps(
                dropdownSearchDecoration: InputDecoration(
                  labelText: "Select Supplier",
                  border: OutlineInputBorder(),
                ),
              ),
              onChanged: (value) {
                setState(() => selectedSupplier = value);
              },
            ),
            SizedBox(height: 16),
            TextField(
              controller: quantityController,
              decoration: InputDecoration(
                labelText: "Quantity",
                border: OutlineInputBorder(),
              ),
              keyboardType: TextInputType.number,
            ),
            SizedBox(height: 20),
            ElevatedButton(onPressed: submitStock, child: Text("Submit")),
          ],
        ),
      ),
    );
  }
}
