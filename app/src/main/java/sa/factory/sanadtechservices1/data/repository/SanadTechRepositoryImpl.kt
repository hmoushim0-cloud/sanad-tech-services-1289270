package sa.factory.sanadtechservices1.data.repository

import sa.factory.sanadtechservices1.domain.repository.SanadTechRepository
import sa.factory.sanadtechservices1.hardware.HardwareSensorDriver
import sa.factory.sanadtechservices1.model.SanadTechItem
import sa.factory.sanadtechservices1.model.SanadTechOrder
import sa.factory.sanadtechservices1.model.SanadTechTelemetry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch

class SanadTechRepositoryImpl(
    private val sensorDriver: HardwareSensorDriver
) : SanadTechRepository {

    private val _catalog = MutableStateFlow<List<SanadTechItem>>(listOf(
        SanadTechItem("1", "الباقة التنفيذية المعتمدة", "تكامل مباشر مع Mada Contactless EMV L2 NFC + GPS Dispatch", 180.0, "معتمد", "الخدمات الرئيسية"),
        SanadTechItem("2", "خدمة المعالجة الفورية", "أداء أصيل بسرعة 60 إطار في الثانية", 95.0, "فوري", "المراقبة الحية"),
        SanadTechItem("3", "باقة الصيانة والدعم السحابي", "تشفير كامل للبيانات المحلية ومزامنة آمنة", 250.0, "شامل", "التقارير الضريبية")
    ))
    override val catalogFlow: Flow<List<SanadTechItem>> = _catalog.asStateFlow()

    private val _cart = MutableStateFlow<List<SanadTechItem>>(emptyList())
    override val activeCart: StateFlow<List<SanadTechItem>> = _cart.asStateFlow()

    override val telemetryFlow: Flow<SanadTechTelemetry> = sensorDriver.observePeripheralStream()
        .map { frame ->
            SanadTechTelemetry(
                peripheralName = frame.peripheralName,
                status = frame.connectionState,
                liveReading = frame.livePayload,
                batteryPct = 96,
                signalDbm = -54
            )
        }

    override fun addToCart(item: SanadTechItem) {
        val current = _cart.value.toMutableList()
        current.add(item)
        _cart.value = current
    }

    override fun removeFromCart(item: SanadTechItem) {
        val current = _cart.value.toMutableList()
        current.remove(item)
        _cart.value = current
    }

    override fun clearCart() {
        _cart.value = emptyList()
    }

    override suspend fun checkout(paymentMethod: String): SanadTechOrder {
        val currentItems = _cart.value.toList()
        val subtotal = currentItems.sumOf { it.priceSar }
        val vat = subtotal * 0.15
        val total = subtotal + vat
        val order = SanadTechOrder(
            orderId = "ORD-${System.currentTimeMillis() % 100000}",
            items = currentItems,
            subtotalSar = subtotal,
            vatSar = vat,
            totalSar = total,
            paymentMethod = paymentMethod
        )
        clearCart()
        return order
    }

    override suspend fun refreshCatalog() {
        // Offline-first sync logic
    }
}
